import psycopg2
import requests
import logging

BASE_URL = "http://localhost:8080/server/api"
USERNAME = "admin@gmail.com"
PASSWORD = "admin"

# ------------------------------------------------------------
# FETCH GROUP LIST FROM SOURCE DATABASE
# ------------------------------------------------------------
def fetch_groups_from_db():
    conn = psycopg2.connect(
        dbname="batch",
        user="orissa",
        password="orissa",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT uuid, name, permanent, eperson_group_id
        FROM epersongroup
        WHERE name NOT IN ('Anonymous','Administrator')
        ORDER BY name;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    groups = []
    for uuid, name, permanent, eperson_group_id in rows:
        groups.append({
            "uuid": str(uuid),
            "name": name,
            "permanent": bool(permanent),
            "eperson_group_id": str(eperson_group_id),
        })
    return groups


def get_logged_in_session():
    session = requests.Session()
    status_resp = session.post(
        f"{BASE_URL}/authn/status",
        headers={
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest"
        }
    )
    status_resp = session.get(f"{BASE_URL}/authn/status")
    print(session.cookies.get_dict())
    
    if status_resp.status_code != 200:
        raise Exception(f"/authn/status failed: {status_resp.status_code} {status_resp.text}")

    csrf_token = session.cookies.get("DSPACE-XSRF-COOKIE") or session.cookies.get("XSRF-TOKEN") or session.cookies.get("csrftoken") 
    DSPACE_XSRF_TOKEN = session.cookies.get("DSPACE-XSRF-TOKEN")

    if not csrf_token:
        raise Exception(f"No CSRF token found in cookies: {session.cookies.get_dict()}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "X-XSRF-TOKEN": csrf_token,
        "DSPACE-XSRF-TOKEN": DSPACE_XSRF_TOKEN
    }
    data = {"user": USERNAME, "password": PASSWORD}
    login_resp = session.post(f"{BASE_URL}/authn/login", headers=headers, data=data)
    
    if login_resp.status_code != 200:
        raise Exception(f"Login failed: {login_resp.status_code} {login_resp.text}")

    jwt = login_resp.headers.get("Authorization")
    if not jwt:
        raise Exception("No Authorization JWT returned from login")
    session.headers.update({
        "Authorization": jwt,
        "X-XSRF-TOKEN": csrf_token
    })

    return session

def _get_csrf_from_session(session):
    token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )
    if token:
        return token

    # Try hitting status to refresh cookies (safe, does not change login)
    try:
        rr = session.get(f"{BASE_URL}/authn/status", headers={"Accept": "application/json"})
        logging.info(f"Refreshed authn/status {rr.status_code}")
    except Exception as e:
        logging.info(f"authn/status refresh failed: {e}")

    token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )
    return token


# ------------------------------------------------------------
# CREATE GROUP
# ------------------------------------------------------------
def create_group(session, group):
    url = f"{BASE_URL}/eperson/groups"   
    csrf_token = _get_csrf_from_session(session)
    headers = {
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf_token,
        "Authorization": session.headers.get("Authorization"),
        "Accept": "application/json"
    }

    payload = {
        "name": group["name"],
        "metadata": {
            "dc.description": [
                {
                    "value": group["name"],
                }
            ]
        }
    }

    resp = session.post(url,headers=headers, json=payload)

    try:
        data = resp.json()
    except:
        data = resp.text

    if resp.status_code in (200, 201):
        print(f"✅ Group created: {group['name']} ({group['uuid']})")
    else:
        print(f"❌ Failed to create group {group['name']}: {resp.status_code} -> {data}")


# ------------------------------------------------------------
def main():
    session = get_logged_in_session()
    groups = fetch_groups_from_db()

    print(f"Found {len(groups)} groups to migrate.")

    for g in groups:
        create_group(session, g)


if __name__ == "__main__":
    main()
