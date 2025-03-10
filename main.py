import json

import pandas as pd
import requests

from utils import(
    generate_jwt,
    get_pending_members,
    get_active_members,
    update_member_status,
    upsert_memebrs
)

from config import GHOST_ADMIN_URL, MEMBER_LIMIT


def fetch():
    token = generate_jwt()
    url = f"{GHOST_ADMIN_URL}/members"
    headers = {'Authorization': f'Ghost {token}'}
    page = 1
    members = []
    while True:
        r = requests.get(f"{url}?limit=all&page={page}", headers=headers)
        if r.status_code == 401:
            headers['Authorization'] = f'Ghost {generate_jwt()}'
            r = requests.get(f"{url}?limit=all&page={page}", headers=headers)
        if r.status_code == 200:
            data = r.json()
            if data['members']:
                members.extend(data['members'])
                page += 1
            else:
                return members
        else:
            r.raise_for_status()


def delete(members):
    members = pd.DataFrame(members)
    total = len(members)
    members = members[members['labels'].apply(
        lambda labels: any(label.get('name') != 'Internal' for label in labels)
    )]
    inactive_members = members[
            (members['subscribed']==False)
            | (members['email_suppression'].apply(lambda x: x['suppressed'] == True))
            | ((members['email_count'] > 4) & (members['email_open_rate'] < 25.0)
        )
    ]
    token = generate_jwt()
    url = f"{GHOST_ADMIN_URL}/members"
    headers = {'Authorization': f'Ghost {token}'}
    deleted = 0
    for member_id in inactive_members['id']:
        r = requests.delete(f"{url}/{member_id}", headers=headers)
        if r.status_code == 401:
            headers['Authorization'] = f'Ghost {generate_jwt()}'
            r = requests.delete(f"{url}/{member_id}", headers=headers)
        if r.status_code == 204:
            deleted += 1
        else:
            continue
    return MEMBER_LIMIT - total + deleted


def create(count):
    pending_members = get_pending_members(count)
    token = generate_jwt()
    url = f"{GHOST_ADMIN_URL}/members"
    headers = {'Authorization': f'Ghost {token}'}
    failed = []
    for _, row in pending_members.iterrows():
        member = {
            'members': [{
                'email': row['email'],
                'labels': [{
                    "name": row['label'],
                    "slug": row['label'].lower(),
                }],
                'note': row['note'] if row['note'] else ''
            }]
        }
        r = requests.post(f"{url}", json=member, headers=headers)
        if r.status_code == 401:
            headers['Authorization'] = f'Ghost {generate_jwt()}'
            r = requests.post(f"{url}", json=member, headers=headers)
        if r.status_code != 201:
            failed.append(row['email'])
    if failed:
        update_member_status(failed, 'failed')


def sync(members):
    members = pd.DataFrame(members)
    members['label'] = members['labels'].apply(
        lambda labels: next(iter(labels), {}).get('name')
    )
    members = members[['email', 'label', 'note', 'created_at']]
    active_members = get_active_members()
    new_members = members[~members['email'].isin(active_members['email'])]
    missing_members = active_members[~active_members['email'].isin(members['email'])]
    if not new_members.empty:
        upsert_memebrs(json.dumps(new_members.to_dict(orient="records")))
    if not missing_members.empty:
        update_member_status(missing_members['email'], 'deleted')


if __name__ == "__main__":
    create(delete(fetch()))
    sync(fetch())