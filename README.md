### Before README:
Vibe coded by gemini-flash-3.5, so a human to human introduction before reading the AI generated README:

Simply it handles the DIRECT and PROXY traffics for you, if a website didn't load in TIMEOUT (3 Seconds), the script will route it to PROXY.

I wanted this to also detects the 403 errors, platform blocked contents (تحریم), so it will test it through the PROXY channel first to see if the error is real or not.

It's not limited to GeoIPs and works well with MasterHttpRelayVPN or **any** other proxies.

# SmartRoute-Proxy
A high-performance, memory-resident local proxy for dynamic ISP censorship bypass.

## Description
SmartRoute-Proxy is an intelligent, low-latency SOCKS/HTTP proxy designed for users navigating restricted network environments. Unlike traditional VPNs that route all traffic through a tunnel, SmartRoute-Proxy intercepts your requests and evaluates the connection status in real-time. 

It proactively probes whether a destination is censored by your ISP or geoblocked by the server, dynamically routing traffic via a direct connection or an upstream tunnel (like NekoRay/sing-box) accordingly. It uses a high-performance in-memory cache to ensure zero-latency routing decisions for subsequent requests.

## Key Features
- **Adaptive Routing:** Automatically differentiates between ISP-level blocking and server-side geoblocks.
- **In-Memory Performance:** All routing decisions are served from RAM, ensuring no disk I/O lag during browsing.
- **Asynchronous Persistence:** Periodically flushes routing states to an SQLite database for long-term session persistence.
- **Subdomain-Aware:** Handles subdomains individually to ensure global services (like Google) are only tunneled when specifically blocked.

## Installation
1. Ensure Python 3.10+ is installed.
2. Install the required dependencies:
```bash
   pip install requests urllib3
```
3. Run the proxy:
```bash
python3 smart_proxy.py
```

## Configuration
- Update NEKORAY_PORT in the script to match your local NekoRay/Xray inbound port.
- Configure your system or browser proxy settings to point to 127.0.0.1:8085.

## How it works

SmartRoute-Proxy acts as an interceptor. By analyzing the handshake and HTTP response codes, it classifies traffic paths without manual list maintenance.
