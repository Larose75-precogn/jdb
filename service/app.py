#!/usr/bin/env python3
"""
jdb_api — JournalDeBanque (Phase 1 : staging des propositions + file de validation).

Rôle (voir 0 JournaldeBanque.pdf §4 : "La proposition est soumise à l'organisation avant
inscription dans le journal") : le mouvement bancaire réel n'entre JAMAIS directement dans le
journal comptable. Il passe d'abord par un STAGING (propositions), où un User habilité le
contrôle, le qualifie, le valide ou le rejette. Seule la validation écrit — via
ledger_api /api/ledger/import — dans le journal de l'organisation (son Own Storage).

Ce service ne réinvente ni le moteur comptable (ledger_api), ni les connectors bancaires
(executor), ni la sanctuarisation des relevés (analyzor/own_storage_releves). Il orchestre :
    executor.fetch-transactions  →  STAGING (propositions)  →  ledger_api.import (validation)

Le staging est un ÉTAT DE TRAVAIL transitoire (avant décision), pas la source de vérité : la
preuve d'origine reste le relevé sanctuarisé (Own Storage), et le journal validé reste dans
le journal.ledger de l'org (Own Storage). Le staging peut donc vivre en local sur le VPS,
comme la copie de travail locale des journaux dans ledger_api/orgs/.
"""
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LEDGER_URL = os.environ.get('LEDGER_URL', 'http://localhost:8080')
EXECUTOR_URL = os.environ.get('EXECUTOR_URL', 'http://localhost:8084')
ANALYZOR_URL = os.environ.get('ANALYZOR_URL', 'http://localhost:8000')
SUBSCRIPTIONS_URL = os.environ.get('SUBSCRIPTIONS_URL', 'http://localhost:8082')
SERVICE_API_KEY = os.environ.get('SERVICE_API_KEY', '')

STAGING_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'staging')
os.makedirs(STAGING_DIR, exist_ok=True)

ORG_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')
# Compte bancaire par défaut (classe 5, PCG). Le numéro précis par établissement pourra être
# porté par la brique Compte (contenu.numero_comptable) plus tard ; défaut générique ici.
DEFAULT_BANK_CODE = '512000'

_locks = {}
_locks_guard = threading.Lock()


def _org_lock(org_id):
    with _locks_guard:
        if org_id not in _locks:
            _locks[org_id] = threading.Lock()
        return _locks[org_id]


def _staging_path(org_id):
    return os.path.join(STAGING_DIR, f'{org_id}.json')


def _load_staging(org_id):
    path = _staging_path(org_id)
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def _save_staging(org_id, props):
    path = _staging_path(org_id)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(props, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


# ── Autorisation (même modèle que ledger_api._authorize_write) ──────────────────

def _resolve_role(org_id, email):
    """Rôle sur l'org via le connector d'auth (subscriptions_api). None si non-membre/erreur."""
    if not email:
        return None
    try:
        r = requests.get(f'{SUBSCRIPTIONS_URL}/api/auth/membership',
                         params={'orgId': org_id, 'email': email},
                         headers={'X-Service-Key': SERVICE_API_KEY}, timeout=5)
        d = r.json()
        return d.get('role') if d.get('isMember') else None
    except Exception:
        return None


# Orgs de DÉMO publiques : accessibles sans auth (données d'exemple, jamais du réel). Même
# esprit que PUBLIC_DEMO_ORG_IDS côté Navigator. jdb_api agit alors comme owner de démo.
DEMO_ORGS = {'jdbshow'}
DEMO_EMAIL = 'demo@structory.ai'


def _require(org_id, data, write=True):
    """Même modèle que ledger_api._authorize_write : l'auth (session/email) est faite EN AMONT
    par le connector (Apps Script), qui appelle ensuite jdb_api avec service-key + userEmail
    (email déjà vérifié). jdb_api fait confiance à ce couple, il ne refait pas l'auth.
    write=True exige editor/owner ; write=False exige juste d'être membre.
    Exception : les orgs de démo publiques sont ouvertes (données d'exemple)."""
    if org_id in DEMO_ORGS:
        return DEMO_EMAIL, 'owner', None
    if request.headers.get('X-Service-Key') != SERVICE_API_KEY:
        return None, None, (jsonify({'success': False, 'errorCode': 'service_key',
                                     'error': 'appelant non autorisé (service-key) — passe par le connector'}), 401)
    if not isinstance(data, dict):
        data = {}
    email = (data.get('userEmail') or request.args.get('userEmail') or '').strip().lower()
    if not email:
        return None, None, (jsonify({'success': False, 'error': 'userEmail manquant'}), 400)
    role = _resolve_role(org_id, email)
    if role is None:
        return None, None, (jsonify({'success': False, 'errorCode': 'not_member',
                                     'error': 'non membre de cette organisation'}), 403)
    if write and role not in ('editor', 'owner'):
        return None, None, (jsonify({'success': False, 'errorCode': 'read_only',
                                     'error': 'droit insuffisant : viewer (lecture seule)',
                                     'role': role}), 403)
    return email, role, None


def _service_key_ok():
    return request.headers.get('X-Service-Key') == SERVICE_API_KEY


# ── Enrichissement : suggestion de contrepartie (réutilise le PCG de ledger_api) ─

def _classify(libelle, sens, org_id):
    try:
        r = requests.get(f'{LEDGER_URL}/api/ledger/classify',
                         params={'libelle': libelle, 'sens': sens, 'orgId': org_id}, timeout=8)
        d = r.json()
        if d.get('success'):
            return {'compte': d['compte'], 'nom': d['nom'], 'confidence': d.get('confidence')}
    except Exception:
        pass
    return {'compte': '471000', 'nom': 'Attente — non classé', 'confidence': 0.0}


def _sens(montant_signe):
    return 'recette' if float(montant_signe) > 0 else 'depense'


# ── Routes ──────────────────────────────────────────────────────────────────────

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'success': True, 'service': 'jdb_api', 'staging_dir': STAGING_DIR})


@app.route('/api/jdb/pull', methods=['POST'])
def pull():
    """Récupère l'historique détaillé des comptes d'une org et le stage en propositions.
    Body: {orgId, userEmail, module?, comptes: [{etablissement, nature, titulaire?, produit?}],
           sanctuarize?: bool}. `comptes` est fourni par l'appelant (Navigator connaît déjà les
           comptes de l'org) — jdb_api ne duplique pas l'énumération des comptes.
    Dédoublonne par source_id contre le staging existant (tous statuts confondus) : une
    transaction déjà proposée/validée/rejetée n'est jamais re-proposée."""
    data = request.get_json() or {}
    org_id = data.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    email, role, err = _require(org_id, data, write=True)
    if err:
        return err
    module = data.get('module')
    comptes = data.get('comptes') or []
    sanctuarize = data.get('sanctuarize', True)
    if not isinstance(comptes, list) or not comptes:
        return jsonify({'success': False, 'error': 'comptes (liste) requis'}), 400

    per_compte = []
    collected = []
    for c in comptes:
        body = {'orgId': org_id, 'module': module,
                'etablissement': c.get('etablissement'), 'nature': c.get('nature'),
                'titulaire': c.get('titulaire'), 'produit': c.get('produit'),
                'sanctuarize': sanctuarize}
        try:
            r = requests.post(f'{EXECUTOR_URL}/api/executor/fetch-transactions', json=body, timeout=60)
            res = r.json()
        except requests.RequestException as e:
            res = {'success': False, 'error': f'executor injoignable : {e}'}
        if res.get('success'):
            for t in res.get('transactions', []):
                t = dict(t)
                t['etablissement'] = res.get('etablissement')
                t['nature'] = res.get('nature')
                t['titulaire'] = res.get('titulaire')
                collected.append(t)
        per_compte.append({'compte': res.get('compte') or c.get('etablissement'),
                           'success': res.get('success', False),
                           'error': res.get('error'),
                           'n': len(res.get('transactions', []) or []),
                           'sanctuarise': res.get('sanctuarise')})

    result = _stage_transactions(org_id, collected)
    result['perCompte'] = per_compte
    return jsonify(result)


@app.route('/api/jdb/inject', methods=['POST'])
def inject():
    """Injection directe de transactions normalisées dans le staging — outil de dev/test
    (backend uniquement, service-key requise), pour exercer la file de validation sans banque
    live. Body: {orgId, transactions: [{date, montant_signe, devise?, libelle, source_id,
    etablissement?, nature?, titulaire?}]}."""
    if not _service_key_ok():
        return jsonify({'success': False, 'error': 'service-key requise'}), 401
    data = request.get_json() or {}
    org_id = data.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    txs = data.get('transactions') or []
    if not isinstance(txs, list) or not txs:
        return jsonify({'success': False, 'error': 'transactions (liste) requis'}), 400
    return jsonify(_stage_transactions(org_id, txs))


def _stage_transactions(org_id, transactions):
    """Insère des transactions normalisées en propositions (statut 'propose'), en dédoublonnant
    par source_id contre tout le staging existant."""
    lock = _org_lock(org_id)
    with lock:
        props = _load_staging(org_id)
        seen = {p.get('source_id') for p in props if p.get('source_id')}
        n_new, n_dup = 0, 0
        for t in transactions:
            sid = t.get('source_id') or f"manual_{uuid.uuid4().hex[:12]}"
            if sid in seen:
                n_dup += 1
                continue
            seen.add(sid)
            props.append({
                'prop_id': uuid.uuid4().hex[:16],
                'source_id': sid,
                'date': (t.get('date') or '')[:10],
                'montant_signe': t.get('montant_signe'),
                'devise': t.get('devise') or 'EUR',
                'libelle': (t.get('libelle') or '').strip(),
                'etablissement': t.get('etablissement'),
                'nature': t.get('nature'),
                'titulaire': t.get('titulaire'),
                'statut': 'propose',
                'created_at': _now(),
                'decided_at': None, 'decided_by': None, 'motif': None,
                'ledger_source': None,
            })
            n_new += 1
        _save_staging(org_id, props)
    return {'success': True, 'new': n_new, 'duplicates': n_dup, 'total': len(props)}


@app.route('/api/jdb/propositions', methods=['GET'])
def propositions():
    """Liste les propositions d'une org. ?statut=propose|valide|rejete (optionnel).
    Enrichit chaque proposition en attente d'une suggestion de contrepartie (PCG ledger_api)
    et du sens (recette/dépense). Lecture : réservé aux membres."""
    org_id = request.args.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    email, role, err = _require(org_id, {}, write=False)
    if err:
        return err
    statut = request.args.get('statut')
    props = _load_staging(org_id)
    out = []
    for p in props:
        if statut and p.get('statut') != statut:
            continue
        p = dict(p)
        if p.get('statut') == 'propose' and p.get('montant_signe') is not None:
            sens = _sens(p['montant_signe'])
            p['sens'] = sens
            p['suggestion'] = _classify(p.get('libelle', ''), sens, org_id)
        out.append(p)
    counts = {}
    for p in props:
        counts[p.get('statut')] = counts.get(p.get('statut'), 0) + 1
    return jsonify({'success': True, 'propositions': out, 'counts': counts, 'role': role})


@app.route('/api/jdb/valider', methods=['POST'])
def valider():
    """Valide des propositions → écrit dans le journal de l'org via ledger_api/import.
    Body: {orgId, userEmail, propIds: [...], overrides?: {propId: {compte, nom}}}.
    Réservé editor/owner (double contrôle : ici ET dans ledger_api/import)."""
    data = request.get_json() or {}
    org_id = data.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    email, role, err = _require(org_id, data, write=True)
    if err:
        return err
    prop_ids = data.get('propIds') or []
    overrides = data.get('overrides') or {}
    if not isinstance(prop_ids, list) or not prop_ids:
        return jsonify({'success': False, 'error': 'propIds (liste) requis'}), 400

    lock = _org_lock(org_id)
    with lock:
        props = _load_staging(org_id)
        by_id = {p['prop_id']: p for p in props}
        targets = [by_id[pid] for pid in prop_ids if pid in by_id and by_id[pid].get('statut') == 'propose']
        if not targets:
            return jsonify({'success': False, 'error': 'aucune proposition en attente pour ces ids'}), 404

        entries = []
        for p in targets:
            m = float(p['montant_signe'])
            sens = _sens(m)
            ov = overrides.get(p['prop_id']) or {}
            if ov.get('compte'):
                cp = {'compte': ov['compte'], 'nom': ov.get('nom') or ov['compte']}
            else:
                cp = _classify(p.get('libelle', ''), sens, org_id)
            bank_label = p.get('etablissement') or 'Banque'
            entries.append({
                'date': (p['date'] or '').replace('-', '/'),
                'libelle': p.get('libelle') or 'Mouvement bancaire',
                'legs': [
                    {'compte': DEFAULT_BANK_CODE, 'label': bank_label, 'amount': m},
                    {'compte': cp['compte'], 'label': cp['nom'], 'amount': -m},
                ],
            })

        payload = {'orgId': org_id, 'userEmail': email, 'source': 'JdB', 'entries': entries}
        try:
            r = requests.post(f'{LEDGER_URL}/api/ledger/import', json=payload,
                              headers={'X-Service-Key': SERVICE_API_KEY}, timeout=30)
            imp = r.json()
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'ledger_api injoignable : {e}'}), 502

        if not imp.get('success'):
            # remonte tel quel (needs_bootstrap 409, read_only 403, déséquilibre 400...)
            status = r.status_code if r.status_code >= 400 else 400
            return jsonify({'success': False, 'stage': 'ledger_import', 'ledger': imp}), status

        stamp = _now()
        for p in targets:
            p['statut'] = 'valide'
            p['decided_at'] = stamp
            p['decided_by'] = email
            p['ledger_source'] = 'JdB'
        _save_staging(org_id, props)

    return jsonify({'success': True, 'validated': len(targets),
                    'nImported': imp.get('nImported'), 'balanceCheck': imp.get('balanceCheck')})


@app.route('/api/jdb/rejeter', methods=['POST'])
def rejeter():
    """Rejette des propositions (n'écrit rien dans le journal). Body: {orgId, userEmail,
    propIds: [...], motif?}. Réservé editor/owner."""
    data = request.get_json() or {}
    org_id = data.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    email, role, err = _require(org_id, data, write=True)
    if err:
        return err
    prop_ids = data.get('propIds') or []
    motif = data.get('motif')
    if not isinstance(prop_ids, list) or not prop_ids:
        return jsonify({'success': False, 'error': 'propIds (liste) requis'}), 400

    lock = _org_lock(org_id)
    with lock:
        props = _load_staging(org_id)
        by_id = {p['prop_id']: p for p in props}
        n = 0
        stamp = _now()
        for pid in prop_ids:
            p = by_id.get(pid)
            if p and p.get('statut') == 'propose':
                p['statut'] = 'rejete'
                p['decided_at'] = stamp
                p['decided_by'] = email
                p['motif'] = motif
                n += 1
        _save_staging(org_id, props)
    return jsonify({'success': True, 'rejected': n})


@app.route('/api/jdb/journal', methods=['GET'])
def journal_de_banque():
    """Vue journal de banque = register ledger-cli filtré sur le(s) compte(s) banque de l'org
    (mécanisme natif ledger-cli, cf. décision d'archi : JdB est une VUE auxiliaire du journal,
    pas un journal séparé). ?orgId=&compte=512 (défaut 512). Lecture : membres."""
    org_id = request.args.get('orgId', '')
    if not ORG_ID_RE.match(org_id):
        return jsonify({'success': False, 'error': 'orgId invalide'}), 400
    email, role, err = _require(org_id, {}, write=False)
    if err:
        return err
    compte = request.args.get('compte', DEFAULT_BANK_CODE[:3])
    if compte.startswith('-'):
        return jsonify({'success': False, 'error': 'filtre invalide'}), 400
    try:
        r = requests.post(f'{LEDGER_URL}/api/ledger/query',
                          json={'orgId': org_id, 'command': 'register', 'filters': [compte]},
                          timeout=15)
        return jsonify(r.json()), r.status_code
    except requests.RequestException as e:
        return jsonify({'success': False, 'error': f'ledger_api injoignable : {e}'}), 502


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8086, debug=False)
