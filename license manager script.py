import os
import requests

# === FILL THESE IN FROM DEVTOOLS ======================================
# 1. Copy the *exact* URL from the "Export CSV" network request
EXPORT_URL = os.getenv(
    "LICENSE_EXPORT_URL",
    "https://license-manager-web-aa.fiskcake.com/licensemanager/admin/usage/export"  # <-- replace with real one
)

# 2. Copy the Cookie header (or Authorization header) from that request.
#    Best practice: put it in an environment variable so it’s not in plain text in the script.
COOKIE_HEADER = os.getenv("LICENSE_SESSION_COOKIE", "")
AUTH_HEADER = os.getenv("LICENSE_AUTH_HEADER", "")  # e.g. "Bearer eyJ0eXAiOiJKV1Qi..."

# =====================================================================

def download_license_csv(output_path: str = "license_usage.csv"):
    if not COOKIE_HEADER and not AUTH_HEADER:
        raise RuntimeError(
            "No auth info set. Set LICENSE_SESSION_COOKIE or LICENSE_AUTH_HEADER "
            "environment variable with the values from DevTools."
        )

    headers = {
        # Only send what you actually saw in DevTools
        # (Don’t invent headers you didn’t see)
    }

    if COOKIE_HEADER:
        headers["Cookie"] = COOKIE_HEADER

    if AUTH_HEADER:
        headers["Authorization"] = AUTH_HEADER

    print(f"Requesting CSV from: {EXPORT_URL}")
    resp = requests.get(EXPORT_URL, headers=headers, stream=True)
    resp.raise_for_status()

    # Try to use filename from Content-Disposition if present
    cd = resp.headers.get("Content-Disposition", "")
    filename = output_path
    if "filename=" in cd:
        # crude parse, but works for standard headers
        filename = cd.split("filename=")[-1].strip('"; ')

    print(f"Saving to: {filename}")
    with open(filename, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print("Download complete.")


if __name__ == "__main__":
    download_license_csv()
