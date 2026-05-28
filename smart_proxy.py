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

# Silence insecure warnings from direct probes
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
DISK_SYNC_INTERVAL = 60    # Save memory cache to disk every 60 seconds if changed

# --- IN-MEMORY CACHE LAYER & LOCKS ---
CACHE_MEMORY = {}       # Layout: { domain: {"status": "DIRECT", "timestamp": 1700000000} }
cache_lock = threading.Lock()
cache_dirty = False     # Flag to track if memory has new updates that aren't on disk yet

# --- ASYNC STORAGE ENGINE ---
def load_db_to_memory():
    """Runs once at startup to load historical data from disk into RAM."""
    global cache_dirty
    if not os.path.exists(DB_FILE):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS domain_cache (
                    domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER
                )
            ''')
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
                    # Only load valid, non-expired cache records
                    if now - timestamp < CACHE_TTL_SECONDS and domain != '443' and domain != '':
                        CACHE_MEMORY[domain] = {"status": status, "timestamp": timestamp}
        print(f"[+] Loaded {len(CACHE_MEMORY)} domains from disk into memory cache.")
    except sqlite3.Error as e:
        print(f"[-] Failed to load database to memory: {e}", file=sys.stderr)

def disk_serializer_worker():
    """Background loop that periodically flushes the memory state to the physical disk."""
    global cache_dirty
    while True:
        time.sleep(DISK_SYNC_INTERVAL)
        if cache_dirty:
            # Take a rapid snapshot under lock to minimize request blocking
            with cache_lock:
                snapshot = list(CACHE_MEMORY.items())
                cache_dirty = False
            
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    cursor = conn.cursor()
                    # Keep database clean by recreating valid records
                    cursor.execute("DROP TABLE IF EXISTS domain_cache")
                    cursor.execute('''
                        CREATE TABLE domain_cache (
                            domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER
                        )
                    ''')
                    cursor.executemany(
                        "INSERT INTO domain_cache (domain, status, timestamp) VALUES (?, ?, ?)",
                        [(domain, data["status"], data["timestamp"]) for domain, data in snapshot]
                    )
                    conn.commit()
                print("[*] Asynchronously saved memory cache snapshot to disk.")
            except sqlite3.Error as e:
                print(f"[-] Background disk save failed: {e}", file=sys.stderr)
                with cache_lock:
                    cache_dirty = True # Retry on next loop iteration

def get_routing_status(domain):
    """Instant lookup from local RAM."""
    now = time.time()
    with cache_lock:
        if domain in CACHE_MEMORY:
            record = CACHE_MEMORY[domain]
            if now - record["timestamp"] < CACHE_TTL_SECONDS:
                return record["status"]
            else:
                del CACHE_MEMORY[domain] # Evict expired record
    return None

def update_routing_status(domain, status):
    """Writes directly to RAM and signals the background worker to sync later."""
    global cache_dirty
    with cache_lock:
        CACHE_MEMORY[domain] = {
            "status": status,
            "timestamp": int(time.time())
        }
        cache_dirty = True

# --- ROUTING ENGINE ---
def get_base_domain(url_or_host):
    if "://" in url_or_host:
        hostname = urlparse(url_or_host).hostname or ""
    else:
        hostname = url_or_host

    if not hostname:
        return ""
    
    hostname = hostname.split(':')[0].lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
        
    return hostname

def probe_domain(domain):
    if not domain or domain.isdigit():
        return "DIRECT"
        
    probe_url = f"https://{domain}"
    print(f"[*] Probing unknown domain: {domain}")

    # Step 1: Try Direct Connection
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"}
        r = requests.head(probe_url, headers=headers, timeout=TIMEOUT_SECONDS, allow_redirects=True, verify=False)
        
        if r.status_code < 400 or r.status_code == 404:
            print(f"[+] {domain} is open directly (Status: {r.status_code})")
            return "DIRECT"
            
        if r.status_code in [403, 451]:
            print(f"[!] Received Status {r.status_code} on direct connection. Double-checking via VPN...")
            
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        print(f"[-] Direct connection to {domain} failed (ISP Blocked). Testing via NekoRay...")
    
    # Step 2: Double-check via NekoRay
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"}
        r_vpn = requests.head(probe_url, headers=headers, proxies=NEKORAY_PROXIES, timeout=TIMEOUT_SECONDS + 2, allow_redirects=True, verify=False)
        
        print(f"[+] {domain} resolved via VPN (Status: {r_vpn.status_code}). Marking as PROXY.")
        return "PROXY"
    except Exception:
        print(f"[!] {domain} completely unreachable globally. Defaulting to PROXY.")
        return "PROXY"

def resolve_route(url_or_host):
    domain = get_base_domain(url_or_host)
    if not domain or domain.isdigit():
        return "DIRECT"
        
    status = get_routing_status(domain)
    if status:
        return status
        
    status = probe_domain(domain)
    update_routing_status(domain, status)
    return status

# --- SOCKET SPLICING FOR TUNNELS ---
def splice_sockets(client_sock, target_sock):
    client_sock.setblocking(False)
    target_sock.setblocking(False)
    while True:
        data_sent = False
        try:
            data = client_sock.recv(16384)
            if not data: break
            target_sock.sendall(data)
            data_sent = True
        except BlockingIOError: pass
        except Exception: break
            
        try:
            data = target_sock.recv(16384)
            if not data: break
            client_sock.setblocking(True) # Ensure full payload flushes to client safely
            client_sock.sendall(data)
            client_sock.setblocking(False)
            data_sent = True
        except BlockingIOError: pass
        except Exception: break
            
        if not data_sent:
            time.sleep(0.001)

# --- PROXY SERVER LAYER ---
class SmartProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass 

    def handle_proxy_request(self):
        url = self.path
        mode = resolve_route(url)
        print(f"[{mode}] HTTP -> {url[:60]}")
        
        headers = {k: v for k, v in self.headers.items() if k.lower() != 'host'}
        proxies = NEKORAY_PROXIES if mode == "PROXY" else None
        
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length) if content_length > 0 else None

            resp = requests.request(
                method=self.command, url=url, headers=headers, data=body,
                proxies=proxies, stream=True, timeout=10.0
            )
            
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() not in ['chunked', 'transfer-encoding']:
                    self.send_header(k, v)
            self.end_headers()
            
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    self.wfile.write(chunk)
        except Exception as e:
            self.send_error(502, f"Proxy HTTP Routing Error: {e}")

    def do_GET(self): self.handle_proxy_request()
    def do_POST(self): self.handle_proxy_request()

    def do_CONNECT(self):
        host_port = self.path
        mode = resolve_route(host_port)
        print(f"[{mode}] CONNECT -> {host_port}")
        
        try:
            host, port_str = host_port.split(":")
            port = int(port_str)
        except ValueError:
            self.send_error(400, "Bad Request")
            return

        try:
            upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream_sock.settimeout(TIMEOUT_SECONDS)
            
            if mode == "PROXY":
                upstream_sock.connect((NEKORAY_HOST, NEKORAY_PORT))
                connect_header = f"CONNECT {host_port} HTTP/1.1\r\nHost: {host_port}\r\n\r\n"
                upstream_sock.sendall(connect_header.encode('utf-8'))
            else:
                upstream_sock.connect((host, port))
                self.send_response(200, "Connection Established")
                self.end_headers()

            splice_sockets(self.connection, upstream_sock)
            
        except Exception as e:
            try: self.send_error(502, f"Routing Error: {e}")
            except: pass
        finally:
            try: upstream_sock.close()
            except: pass

class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    # 1. Warm cache from historical disk file
    load_db_to_memory()
    
    # 2. Spawn the disk serializer as a background daemon thread
    sync_thread = threading.Thread(target=disk_serializer_worker, daemon=True)
    sync_thread.start()
    
    print("--- High-Performance Memory Proxy Active ---")
    print(f"Listening locally at http://{LISTEN_HOST}:{LISTEN_PORT}")
    print("All routing matches served directly from RAM.")
    print("Press Ctrl+C to terminate.")
    
    server = ThreadedHTTPServer((LISTEN_HOST, LISTEN_PORT), SmartProxyHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nFlushing cache down to disk before termination...")
        # Force a final manual write so no browsing data from this session is lost
        if cache_dirty:
            with sqlite3.connect(DB_FILE) as c:
                c.execute("DROP TABLE IF EXISTS domain_cache")
                c.execute("CREATE TABLE domain_cache (domain TEXT PRIMARY KEY, status TEXT, timestamp INTEGER)")
                c.executemany(
                    "INSERT INTO domain_cache (domain, status, timestamp) VALUES (?, ?, ?)",
                    [(d, data["status"], data["timestamp"]) for d, data in CACHE_MEMORY.items()]
                )
                c.commit()
        print("Shutdown complete.")
        sys.exit(0)
