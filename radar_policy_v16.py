"""Policy 16. Egypt decisions delegate unchanged; international evidence is scoped.

No regex or advisory score is a final GPT fit score. Unclear strong remote roles
are delivered for eligibility review rather than falsely labelled eligible.
"""
from __future__ import annotations
import hashlib
import re
from contextlib import contextmanager
from typing import Any
import radar_pipeline_v13 as search
import targeted_queue_v14 as legacy
import targeted_queue_v15 as prior

POLICY_VERSION = 16
_OLD_CLASSIFY = prior.classify_candidate
_OLD_MODEL = prior.work_model
_OLD_SCOPE = prior.remote_scope
_OLD_ANNOTATION = prior.remote_eligibility_annotation
_OLD_COMPACT = search.compact_candidate

HARD_RULES = (
    r'\bonly\s+(?:EU|EEA|British|UK|US|U\.S\.)\s+(?:nationals|citizens)\b',
    r'\b(?:EU|EEA|British|UK|US|U\.S\.)\s+(?:nationals|citizens)\s+only\b',
    r'\b(?:security\s+clearance|citizenship)\s*:\s*(?:British|UK|US|U\.S\.)\s*(?:National|Citizen)?\b',
    r'\b(?:active|current|minimum)\s+(?:security\s+)?clearance\s*(?:required)?\s*[:=-]?\s*(?:TS[./ ]?SCI|SC|DV|secret|top secret)\b',
    r'\blocal candidates only\b',
    r'\bmust (?:already |currently )?(?:have|hold|possess) (?:the )?(?:right|permission|authori[sz]ation) to work\b',
)
NEGATIVE_MOBILITY = (
    r'\b(?:no|without)\s+(?:visa\s+)?sponsorship\b',
    r'\b(?:unable|cannot|can\x27t|do not|does not|will not|not able)\b[^.;\n]{0,75}\b(?:sponsor|sponsorship)\b',
    r'\b(?:visa\s+)?sponsorship\b[^.;\n]{0,50}\b(?:not available|not provided|not offered|unavailable)\b',
    r'\bno\s+(?:international\s+)?relocation\b',
    r'\brelocation\b[^.;\n]{0,80}\b(?:not be available|not available|not provided|not offered|unavailable)\b',
)
POSITIVE_MOBILITY = (
    r'\b(?:offer|provide|provides|offering|available|support|supports)\b[^.;\n]{0,65}\b(?:visa sponsorship|work permit|relocation)\b',
    r'\b(?:visa sponsorship|relocation assistance|relocation support|relocation package|work permit support)\b',
    r'\b(?:welcome|accept|open to|consider)\b[^.;\n]{0,50}\b(?:international|overseas)\s+(?:applicants|candidates)\b',
)
COUNTRY_REMOTE = re.compile(
    r'\b(?:remote\s+(?:only\s+)?(?:within|in|from|across)|work from anywhere\s+(?:in|within|across)|(?:must|need to)\s+(?:be based|reside|live)\s+in)\s+(?:the\s+)?([^.;\n]{2,100})', re.I)
COUNTRIES = re.compile(r'\b(?:republic of ireland|ireland|united kingdom|uk|united states|usa|u\.s\.|canada|india|georgia|australia|south africa|germany|france|spain|portugal|poland|switzerland|europe|eu|eea)\b', re.I)
ELIGIBLE = re.compile(r'\b(?:egypt|emea|mena|middle east|worldwide|any country|anywhere in the world)\b', re.I)


def text(candidate: dict[str, Any]) -> str:
    return '\n'.join(str(candidate.get(k) or '') for k in ('title', 'location', 'description'))


def hits(patterns, value: str) -> list[str]:
    return list(dict.fromkeys(m.group(0).strip() for p in patterns for m in re.finditer(p, value, re.I)))


def mobility(candidate: dict[str, Any]) -> dict[str, Any]:
    value = text(candidate)
    negative = hits(NEGATIVE_MOBILITY, value)
    positive = [] if negative else hits(POSITIVE_MOBILITY, value)
    return {'status': 'not_offered_or_restricted' if negative else 'explicit_support_or_overseas_hiring' if positive else 'unconfirmed', 'evidence': (negative or positive)[:4]}


def work_model(candidate: dict[str, Any]):
    value = text(candidate)
    metadata = str(candidate.get('workplace_type') or candidate.get('workplaceType') or '').lower()
    if metadata in {'onsite', 'on-site', 'hybrid', 'remote'}:
        return ('onsite' if metadata in {'onsite', 'on-site'} else metadata), ['explicit workplace metadata']
    patterns = (
        r'\blocation\s*:[^.;\n]{0,90}[-,]\s*on[- ]?site\b',
        r'\b[1-5]\s+days?\s+(?:per week\s+)?on[- ]?site\b',
        r'\b(?:modalit[a\u00e0]|work regime|work model)\s*:[^.;\n]{0,50}\b(?:on[- ]?site|hybrid|ibrido)\b',
    )
    found = hits(patterns, value)
    if found:
        return ('hybrid' if re.search(r'hybrid|ibrido', ' '.join(found), re.I) else 'onsite'), found
    # The original title and role patterns still apply, but inspect the whole
    # description rather than losing a workplace requirement in its footer.
    old, evidence = _OLD_MODEL(candidate)
    if old != 'unknown':
        return old, evidence
    found = hits((r'\b(?:fully|100%)\s+remote\b', r'\b(?:role|position|job)\s+is\s+remote\b', r'\bwork remotely\b'), value)
    return ('remote', found) if found else ('unknown', [])


def remote_scope(candidate: dict[str, Any]):
    value = text(candidate)
    for match in COUNTRY_REMOTE.finditer(value):
        qualifier = match.group(1)
        if COUNTRIES.search(qualifier) and not ELIGIBLE.search(qualifier):
            return 'country_limited', [match.group(0)]
    # Region metadata can describe the hiring location even when the prose does
    # not repeat it. Do not confuse South Africa or Europe-only with all Africa.
    location = str(candidate.get('location') or '').strip().lower()
    model, _ = work_model(candidate)
    if model == 'remote':
        for key, scope in [('emea', 'emea'), ('mena', 'mena'), ('middle east', 'mena'), ('worldwide', 'global'), ('global', 'global'), ('africa', 'africa'), ('egypt', 'egypt')]:
            if location in {key, 'remote - ' + key, 'remote ' + key, key + ' (remote)'}:
                return scope, ['explicit remote hiring location: ' + location]
    return _OLD_SCOPE(candidate)


def annotation(candidate: dict[str, Any]) -> dict[str, Any]:
    model, model_evidence = work_model(candidate)
    scope, scope_evidence = remote_scope(candidate)
    # Keep legacy restrictions, but absence of sponsorship is not a barrier to
    # a job genuinely performed from Egypt without relocation.
    original = _OLD_ANNOTATION(candidate)
    restrictions = [s for s in original.get('restriction_signals', []) if s not in {'no sponsorship', 'without sponsorship'}]
    restrictions += hits(HARD_RULES, text(candidate))
    if scope == 'country_limited':
        restrictions += scope_evidence
    if restrictions:
        status, confidence = 'explicit_location_or_work_authorization_restriction', 'high'
    elif model in {'onsite', 'hybrid'}:
        status, confidence = 'explicit_non_remote', 'high'
    elif model == 'remote' and scope in prior.ELIGIBLE_SCOPES:
        status, confidence = 'explicit_egypt_emea_or_global_signal', 'high'
    else:
        status, confidence = 'requires_full_review', 'low'
    return {'status': status, 'confidence': confidence, 'work_model': model, 'scope': scope,
            'eligible_signals': scope_evidence if scope in prior.ELIGIBLE_SCOPES else [],
            'restriction_signals': list(dict.fromkeys(restrictions)),
            'non_remote_signals': model_evidence if model in {'onsite', 'hybrid'} else []}


def classify(candidate: dict[str, Any]):
    # User requirement: retain EXACT prior broad Egypt admission, including
    # sparse descriptions, mixed titles and incidental skills for GPT to review.
    if search.is_egypt_candidate(candidate):
        return _OLD_CLASSIFY(candidate)
    if not candidate.get('advisory_it_evidence'):
        return None, 'no_it_evidence'
    core = legacy._core_evidence(candidate)
    if not core['moderate']:
        return None, 'weak_core_infrastructure_evidence'
    a = annotation(candidate)
    if a['restriction_signals']:
        return None, 'explicit_location_work_authorization_or_clearance_block'
    negative_roles = legacy._labels(candidate, 'negative_hits')
    if negative_roles & {'Software Development', 'Presales/Sales', 'Data/AI'} and not core['title_roles']:
        return None, 'international_role_not_infrastructure_owned'
    if a['work_model'] == 'remote':
        if a['scope'] in prior.ELIGIBLE_SCOPES:
            return 'remote', 'explicit_remote_eligibility_for_egypt_or_broader_region'
        if core['strong']:
            return 'remote', 'strong_remote_eligibility_unconfirmed_requires_gpt'
        return None, 'country_specific_or_ambiguous_global_remote_eligibility'
    m = mobility(candidate)
    if m['status'] == 'not_offered_or_restricted':
        return None, 'explicit_no_relocation_or_sponsorship'
    if m['status'] == 'explicit_support_or_overseas_hiring':
        return 'relocation', 'explicit_visa_or_relocation_support'
    # Speculation is reserved for genuinely broad technical overlap, not a
    # country name or a single Azure/Google Workspace keyword.
    exceptional = bool(core['title_roles']) and len(core['skills']) >= 5
    mena = legacy._contains_marker(legacy._normalized_location(candidate), prior.MENA_PHYSICAL_LOCATION_MARKERS)
    if exceptional or (mena and core['strong'] and len(core['skills']) >= 3):
        return 'relocation', 'relocation_possible_sponsorship_unconfirmed'
    return None, 'nonremote_without_credible_relocation_path'


def compact(candidate, source_part, excerpt_chars=500):
    result = _OLD_COMPACT(candidate, source_part, excerpt_chars)
    a = annotation(candidate)
    tier = candidate.get('delivery_tier') or classify(candidate)[0]
    result['remote_eligibility'] = a
    result['priority_lane'] = ('egypt' if search.is_egypt_candidate(candidate) else 'relocation' if tier == 'relocation' else 'remote_eligibility_unconfirmed' if a['scope'] not in prior.ELIGIBLE_SCOPES else 'remote_' + str(a['scope']))
    result['mobility'] = mobility(candidate)
    value = str(candidate.get('description') or '')
    sentences = re.split(r'(?<=[.!?])\s+|\n+', value)
    eligibility = [s.strip() for s in sentences if re.search(r'visa|sponsor|relocat|national|citizen|clearance|remote (?:within|in|across)|right to work|must be based|on[- ]?site|hybrid', s, re.I)]
    duties = [s.strip() for s in sentences if re.search(r'administ|responsib|maintain|troubleshoot|requirements|qualifications|years.{0,20}experience', s, re.I)]
    result['eligibility_evidence'] = [s[:300] for s in eligibility[:4]]
    result['description_excerpt'] = ' '.join(dict.fromkeys(duties or sentences))[:max(500, int(excerpt_chars))]
    result['description_sha256'] = hashlib.sha256(value.encode()).hexdigest()
    fingerprint = '\n'.join(re.sub(r'\s+', ' ', str(candidate.get(k) or '')).strip().lower() for k in ('company', 'title', 'description'))
    result['possible_duplicate_group'] = hashlib.sha256(fingerprint.encode()).hexdigest()[:16]
    result['first_seen_utc'] = candidate.get('first_seen_utc')
    result['freshness_verification'] = candidate.get('freshness_verification')
    return result


@contextmanager
def installed():
    changes = [(prior, 'POLICY_VERSION', POLICY_VERSION), (prior, 'classify_candidate', classify),
               (prior, 'work_model', work_model), (prior, 'remote_scope', remote_scope),
               (prior, 'remote_eligibility_annotation', annotation), (search, 'compact_candidate', compact)]
    saved = [(module, key, getattr(module, key)) for module, key, _ in changes]
    try:
        for module, key, value in changes:
            setattr(module, key, value)
        yield
    finally:
        for module, key, value in reversed(saved):
            setattr(module, key, value)
