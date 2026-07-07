import os
import sys
import time
import sqlite3
import socket
import threading
import socketserver
import http.server
import requests
from urllib.parse import urlparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8085

NEKORAY_HOST = "127.0.0.1"
NEKORAY_PORT = 7777
NEKORAY_PROXIES = {
    "http": f"http://{NEKORAY_HOST}:{NEKORAY_PORT}",
    "https": f"http://{NEKORAY_HOST}:{NEKORAY_PORT}"
}

DB_FILE = "proxy_resolver.db"
CACHE_TTL_SECONDS = 43200  # 12 Hours
TIMEOUT_SECONDS = 3.0
DISK_SYNC_INTERVAL = 30    

CACHE_MEMORY = {}       
cache_lock = threading.Lock()
cache_dirty = False     

# --- IN-MEMORY ASYNC STORAGE ---
def load_db_to_memory():
    global cache_dirty
    if not os.path.exists(DB_FILE):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS domain_cache (domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER)')
            conn.commit()
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT domain, status, timestamp FROM domain_cache")
            rows = cursor.fetchall()
            now = time.time()
            with cache_lock:
                for domain, status, timestamp in rows:
                    if now - timestamp < CACHE_TTL_SECONDS and domain:
                        CACHE_MEMORY[domain] = {"status": status, "timestamp": timestamp}
        print(f"[+] Loaded {len(CACHE_MEMORY)} domains into memory cache.")
    except sqlite3.Error as e:
        print(f"[-] Database initialization failure: {e}", file=sys.stderr)

def disk_serializer_worker():
    global cache_dirty
    while True:
        time.sleep(DISK_SYNC_INTERVAL)
        if cache_dirty:
            with cache_lock:
                snapshot = list(CACHE_MEMORY.items())
                cache_dirty = False
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    cursor = conn.cursor()
                    cursor.execute("DROP TABLE IF EXISTS domain_cache")
                    cursor.execute('CREATE TABLE domain_cache (domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER)')
                    cursor.executemany("INSERT INTO domain_cache (domain, status, timestamp) VALUES (?, ?, ?)",
                                       [(domain, data["status"], data["timestamp"]) for domain, data in snapshot])
                    conn.commit()
            except sqlite3.Error as e:
                print(f"[-] Background disk save failed: {e}", file=sys.stderr)
                with cache_lock: cache_dirty = True

def get_routing_status(domain):
    now = time.time()
    with cache_lock:
        if domain in CACHE_MEMORY:
            record = CACHE_MEMORY[domain]
            if now - record["timestamp"] < CACHE_TTL_SECONDS:
                return record["status"]
            else:
                del CACHE_MEMORY[domain]
    return None

def update_routing_status(domain, status):
    global cache_dirty
    with cache_lock:
        CACHE_MEMORY[domain] = {"status": status, "timestamp": int(time.time())}
        cache_dirty = True

# --- ROUTING ENGINE ---
def get_base_domain(url_or_host):
    if "://" in url_or_host:
        hostname = urlparse(url_or_host).hostname or ""
    else:
        hostname = url_or_host
    if not hostname: return ""
    hostname = hostname.split(':')[0].lower().strip()
    if hostname.startswith("www."): hostname = hostname[4:]
    return hostname

def async_probe_worker(domain):
    probe_url = f"https://{domain}"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"}
        r = requests.head(probe_url, headers=headers, timeout=TIMEOUT_SECONDS, allow_redirects=True, verify=False)
        if r.status_code < 400 or r.status_code == 404:
            print(f"[+] {domain} -> DIRECT")
            update_routing_status(domain, "DIRECT")
            return
    except Exception:
        pass
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"}
        requests.head(probe_url, headers=headers, proxies=NEKORAY_PROXIES, timeout=TIMEOUT_SECONDS + 2, allow_redirects=True, verify=False)
        print(f"[▲] {domain} -> PROXY (via NekoRay)")
        update_routing_status(domain, "PROXY")
    except Exception:
        print(f"[▲] {domain} -> PROXY (Fallback)")
        update_routing_status(domain, "PROXY")

def resolve_route(url_or_host):
    domain = get_base_domain(url_or_host)
    if not domain or domain.isdigit() or domain in ["localhost", "127.0.0.1"]:
        return "DIRECT"
    status = get_routing_status(domain)
    if status: return status
    
    # Track when a completely unmapped domain hits the system
    print(f"[*] Inspecting new domain: {domain}")
    update_routing_status(domain, "PROXY")
    threading.Thread(target=async_probe_worker, args=(domain,), daemon=True).start()
    return "PROXY"

# --- ROCK-SOLID TUNNEL PIPELINE ---
def pipe_sockets(sock1, sock2):
    def copy_directional(src, dst):
        try:
            while True:
                data = src.recv(32768)
                if not data: break
                dst.sendall(data)
        except Exception:
            pass
        finally:
            try: dst.close()
            except: pass
            try: src.close()
            except: pass

    t = threading.Thread(target=copy_directional, args=(sock1, sock2), daemon=True)
    t.start()
    copy_directional(sock2, sock1)

# --- ENGINE LAYER ---
class SmartProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass 

    def handle_proxy_request(self):
        url = self.path
        if not url.startswith("http"):
            url = f"http://{self.headers['Host']}{url}"
            
        mode = resolve_route(url)
        headers = {k: v for k, v in self.headers.items() if k.lower() != 'host'}
        proxies = NEKORAY_PROXIES if mode == "PROXY" else None
        
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length) if content_length > 0 else None

            resp = requests.request(
                method=self.command, url=url, headers=headers, data=body,
                proxies=proxies, stream=True, timeout=15.0
            )
            
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() not in ['chunked', 'transfer-encoding']:
                    self.send_header(k, v)
            self.end_headers()
            
            for chunk in resp.iter_content(chunk_size=16384):
                if chunk:
                    try: self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError): break
        except Exception as e:
            try: self.send_error(502, f"Proxy Engine Error: {e}")
            except: pass

    def do_GET(self): self.handle_proxy_request()
    def do_POST(self): self.handle_proxy_request()
    def do_OPTIONS(self): self.handle_proxy_request()
    def do_PUT(self): self.handle_proxy_request()
    def do_DELETE(self): self.handle_proxy_request()
    def do_PATCH(self): self.handle_proxy_request()

    def do_CONNECT(self):
        host_port = self.path
        mode = resolve_route(host_port)
        
        try:
            host, port_str = host_port.split(":")
            port = int(port_str)
        except ValueError:
            return

        try:
            upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream_sock.settimeout(TIMEOUT_SECONDS + 2)
            
            if mode == "PROXY":
                upstream_sock.connect((NEKORAY_HOST, NEKORAY_PORT))
                connect_header = f"CONNECT {host_port} HTTP/1.1\r\nHost: {host_port}\r\n\r\n"
                upstream_sock.sendall(connect_header.encode('utf-8'))
            else:
                upstream_sock.connect((host, port))
                self.wfile.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                self.wfile.flush()

            upstream_sock.settimeout(None)
            self.connection.settimeout(None)
            
            pipe_sockets(self.connection, upstream_sock)
        except Exception:
            pass

class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True  

if __name__ == "__main__":
    load_db_to_memory()
    sync_thread = threading.Thread(target=disk_serializer_worker, daemon=True)
    sync_thread.start()
    
    print("--- Smart Proxy Active ---")
    print(f"Running locally at http://{LISTEN_HOST}:{LISTEN_PORT}")
    
    server = ThreadedHTTPServer((LISTEN_HOST, LISTEN_PORT), SmartProxyHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nFlushing cache down to disk before termination...")
        if cache_dirty:
            with sqlite3.connect(DB_FILE) as c:
                c.execute("DROP TABLE IF EXISTS domain_cache")
                c.execute("CREATE TABLE domain_cache (domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER)")
                c.executemany("INSERT INTO domain_cache (domain, status, timestamp) VALUES (?, ?, ?)",
                               [(d, data["status"], data["timestamp"]) for d, data in CACHE_MEMORY.items()])
                c.commit()
        print("Shutdown complete.")
        sys.exit(0)
