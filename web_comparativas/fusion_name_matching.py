'''Resolución segura de identidad comercial por nombre contra los padrones de Fusión.

La identidad se resuelve siempre desde ``cartera_operadores`` y
``cartera_vendedores``. El usuario solo conserva el modo de vinculación y, si el
Admin tuvo que desambiguar, la firma normalizada elegida; nunca se copian cuentas.
'''
from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from itertools import permutations
import re
import unicodedata

from sqlalchemy.orm import Session

from web_comparativas.models import CarteraOperador, CarteraVendedor


AUTO_SCORE = 0.94
AUTO_TOKEN_FLOOR = 0.88
AUTO_MARGIN = 0.08
CANDIDATE_SCORE = 0.80
MERGE_PREFIX = 'merge:'


def normalize_person_name(value: str | None) -> str:
    text = unicodedata.normalize('NFKD', value or '')
    text = ''.join(ch for ch in text if not unicodedata.combining(ch)).lower()
    return ' '.join(re.findall(r'[a-z0-9]+', text))


def person_signature(value: str | None) -> str:
    return ' '.join(sorted(normalize_person_name(value).split()))


def _similarity(query: str, candidate: str) -> tuple[float, float]:
    '''Devuelve (score compuesto, peor token emparejado), sin depender del orden.'''
    left = normalize_person_name(query).split()
    right = normalize_person_name(candidate).split()
    if not left or not right:
        return 0.0, 0.0
    # Evita crecimiento factorial ante un nombre anómalo o manipulado. Los nombres
    # personales reales auditados tienen hasta cinco tokens; un valor más largo no
    # se vincula por fuzzy y queda fail-closed (la igualdad exacta se evalúa antes).
    if max(len(left), len(right)) > 8:
        return 0.0, 0.0
    if len(left) > len(right):
        left, right = right, left

    best_average = 0.0
    best_floor = 0.0
    for indexes in permutations(range(len(right)), len(left)):
        ratios = [SequenceMatcher(None, token, right[index]).ratio() for token, index in zip(left, indexes)]
        average = sum(ratios) / max(len(left), len(right))
        if average > best_average:
            best_average = average
            best_floor = min(ratios)

    chars = SequenceMatcher(None, person_signature(query), person_signature(candidate)).ratio()
    return 0.65 * best_average + 0.35 * chars, best_floor


@dataclass
class FusionIdentity:
    key: str
    names: set[str] = field(default_factory=set)
    operator_codes: set[str] = field(default_factory=set)
    seller_codes: set[str] = field(default_factory=set)
    signatures: set[str] = field(default_factory=set)

    @property
    def display_name(self) -> str:
        return sorted(self.names, key=lambda value: (len(value), value))[0]

    def as_dict(self, score: float | None = None) -> dict:
        data = {
            'key': self.key,
            'name': self.display_name,
            'aliases': sorted(self.names),
            'operator_codes': sorted(self.operator_codes),
            'seller_codes': sorted(self.seller_codes),
        }
        if score is not None:
            data['score'] = round(score, 4)
        return data


def _can_merge_cross_source(left: str, right: str) -> bool:
    '''Une variantes casi idénticas solo entre padrones y con evidencia fuerte.'''
    lt = person_signature(left).split()
    rt = person_signature(right).split()
    if len(lt) < 2 or len(lt) != len(rt):
        return False
    ratios = [SequenceMatcher(None, a, b).ratio() for a, b in zip(lt, rt)]
    different = sum(ratio < 1.0 for ratio in ratios)
    score, floor = _similarity(left, right)
    return different == 1 and any(ratio == 1.0 for ratio in ratios) and score >= 0.95 and floor >= 0.90


def fusion_identities(db: Session) -> list[FusionIdentity]:
    op_rows = db.query(CarteraOperador.operador_nombre, CarteraOperador.operador_codigo).distinct().all()
    seller_rows = db.query(CarteraVendedor.vendedor_nombre, CarteraVendedor.vendedor_codigo).distinct().all()
    groups: dict[str, FusionIdentity] = {}

    def add(name: str | None, code: str | None, source: str) -> None:
        signature = person_signature(name)
        code = str(code or '').strip()
        if not signature or not code:
            return
        group = groups.setdefault(signature, FusionIdentity(key=signature))
        group.names.add(str(name).strip())
        group.signatures.add(signature)
        (group.operator_codes if source == 'operator' else group.seller_codes).add(code)

    for name, code in op_rows:
        add(name, code, 'operator')
    for name, code in seller_rows:
        add(name, code, 'seller')
    return sorted(groups.values(), key=lambda group: group.display_name)


def _merge_key(left: FusionIdentity, right: FusionIdentity) -> str:
    return MERGE_PREFIX + '|'.join(sorted((left.key, right.key)))


def _combined_identity(left: FusionIdentity, right: FusionIdentity) -> FusionIdentity:
    return FusionIdentity(
        key=_merge_key(left, right),
        names=set(left.names) | set(right.names),
        operator_codes=set(left.operator_codes) | set(right.operator_codes),
        seller_codes=set(left.seller_codes) | set(right.seller_codes),
        signatures=set(left.signatures) | set(right.signatures),
    )


def _cross_source_merge_pairs(identities: list[FusionIdentity]) -> list[tuple[FusionIdentity, FusionIdentity]]:
    '''Propone variantes cruzadas; nunca las une sin confirmación persistida.'''
    operators = [item for item in identities if item.operator_codes and not item.seller_codes]
    sellers = [item for item in identities if item.seller_codes and not item.operator_codes]
    possible: dict[str, list[FusionIdentity]] = {}
    reverse: dict[str, list[FusionIdentity]] = {}
    for operator in operators:
        for seller in sellers:
            if any(_can_merge_cross_source(a, b) for a in operator.names for b in seller.names):
                possible.setdefault(operator.key, []).append(seller)
                reverse.setdefault(seller.key, []).append(operator)
    pairs = []
    for operator in operators:
        matches = possible.get(operator.key, [])
        if len(matches) == 1 and len(reverse.get(matches[0].key, [])) == 1:
            pairs.append((operator, matches[0]))
    return pairs


def resolve_fusion_identity(
    db: Session,
    user_name: str | None,
    selected_signature: str | None = None,
) -> dict:
    identities = fusion_identities(db)
    merge_pairs = _cross_source_merge_pairs(identities)
    selected_raw = (selected_signature or '').strip()
    if selected_raw.startswith(MERGE_PREFIX):
        for left, right in merge_pairs:
            if selected_raw == _merge_key(left, right):
                left_score = max((_similarity(user_name or '', alias)[0] for alias in left.names), default=0.0)
                right_score = max((_similarity(user_name or '', alias)[0] for alias in right.names), default=0.0)
                if left_score < CANDIDATE_SCORE or right_score < CANDIDATE_SCORE:
                    return {'status': 'not_found', 'automatic': False, 'match': None, 'candidates': []}
                combined = _combined_identity(left, right)
                return {
                    'status': 'merge_confirmed', 'automatic': False,
                    'match': combined, 'candidates': [], 'merge_key': combined.key,
                }
        return {'status': 'not_found', 'automatic': False, 'match': None, 'candidates': []}

    selected = person_signature(selected_raw)
    if selected:
        for identity in identities:
            if selected in identity.signatures:
                score = max((_similarity(user_name or '', alias)[0] for alias in identity.names), default=0.0)
                if score >= CANDIDATE_SCORE:
                    return {'status': 'selected', 'automatic': False, 'match': identity, 'candidates': []}
        return {'status': 'not_found', 'automatic': False, 'match': None, 'candidates': []}

    signature = person_signature(user_name)
    if len(signature.split()) < 2:
        return {'status': 'not_found', 'automatic': False, 'match': None, 'candidates': []}

    ranked: list[tuple[float, float, FusionIdentity]] = []
    for identity in identities:
        scored = [_similarity(user_name or '', alias) for alias in identity.names]
        score, floor = max(scored, default=(0.0, 0.0))
        if score >= CANDIDATE_SCORE:
            ranked.append((score, floor, identity))
    ranked.sort(key=lambda item: (-item[0], item[2].display_name))

    # Si el nombre apunta a dos variantes distintas, una por padrón, nunca se
    # combinan por score. Se propone una única unión recíproca y queda fail-closed
    # hasta que el Admin persista exactamente su merge_key.
    score_by_key = {identity.key: (score, floor) for score, floor, identity in ranked}
    merge_proposals = []
    for left, right in merge_pairs:
        left_score = score_by_key.get(left.key, (0.0, 0.0))[0]
        right_score = score_by_key.get(right.key, (0.0, 0.0))[0]
        if left_score >= CANDIDATE_SCORE and right_score >= CANDIDATE_SCORE:
            merge_proposals.append((max(left_score, right_score), left, right, left_score, right_score))
    merge_proposals.sort(key=lambda item: -item[0])
    if len(merge_proposals) == 1:
        _best, left, right, left_score, right_score = merge_proposals[0]
        return {
            'status': 'merge_confirmation_required',
            'automatic': False,
            'match': None,
            'merge_key': _merge_key(left, right),
            'candidates': [left.as_dict(left_score), right.as_dict(right_score)],
        }
    if len(merge_proposals) > 1:
        candidates = {}
        for _best, left, right, left_score, right_score in merge_proposals:
            candidates[left.key] = left.as_dict(left_score)
            candidates[right.key] = right.as_dict(right_score)
        return {
            'status': 'ambiguous', 'automatic': False, 'match': None,
            'candidates': list(candidates.values())[:5],
        }

    exact = [identity for identity in identities if signature in identity.signatures]
    if len(exact) == 1:
        return {'status': 'exact', 'automatic': True, 'match': exact[0], 'candidates': []}

    if ranked:
        top_score, top_floor, top_identity = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else 0.0
        if top_score >= AUTO_SCORE and top_floor >= AUTO_TOKEN_FLOOR and top_score - second_score >= AUTO_MARGIN:
            return {'status': 'confident', 'automatic': True, 'match': top_identity, 'candidates': []}

    candidates = [identity.as_dict(score) for score, _floor, identity in ranked[:5]]
    return {
        'status': 'ambiguous' if candidates else 'not_found',
        'automatic': False,
        'match': None,
        'candidates': candidates,
    }


def fusion_codes_for_user(db: Session, user) -> tuple[set[str], set[str], dict]:
    if not bool(getattr(user, 'cartera_fusion_enabled', False)):
        return set(), set(), {'status': 'disabled', 'match': None, 'candidates': []}
    result = resolve_fusion_identity(
        db,
        getattr(user, 'name', None) or getattr(user, 'full_name', None),
        getattr(user, 'cartera_fusion_identidad', None),
    )
    match = result.get('match')
    if match is None:
        return set(), set(), result
    return set(match.operator_codes), set(match.seller_codes), result


def fusion_account_codes(
    db: Session,
    identity: FusionIdentity,
    allowed_units: set[str] | None = None,
) -> set[str]:
    operator_accounts = {
        row[0]
        for row in db.query(CarteraOperador.codigo_cliente)
        .filter(CarteraOperador.operador_codigo.in_(identity.operator_codes))
        .distinct()
        .all()
    } if identity.operator_codes else set()
    seller_accounts = {
        row[0]
        for row in db.query(CarteraVendedor.codigo_cliente)
        .filter(CarteraVendedor.vendedor_codigo.in_(identity.seller_codes))
        .distinct()
        .all()
    } if identity.seller_codes else set()
    accounts = operator_accounts | seller_accounts
    if allowed_units is not None:
        if not allowed_units:
            return set()
        in_units = {
            row[0]
            for row in db.query(CarteraVendedor.codigo_cliente)
            .filter(CarteraVendedor.unineg.in_(allowed_units))
            .distinct()
            .all()
        }
        accounts &= in_units
    return accounts
