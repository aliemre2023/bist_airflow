import re
from jellyfish import jaro_winkler_similarity

def clean_company_name(name):
    """Şirket ünvanından A.Ş., A.S., vb. temizler, küçük harfe çevirir"""
    name = str(name).upper()
    # A.Ş., A.S., LTD., TİC., SAN. gibi ekleri temizle
    suffixes = [
        r"A\.Ş\.?", r"A\.S\.?", r"LTD\.?", r"ŞTİ\.?",
        r"TİC\.?", r"SAN\.?", r"VE", r"İŞL\.?", r"PAZ\.?"
    ]
    for suffix in suffixes:
        name = re.sub(suffix, "", name)
    # Fazla boşlukları temizle
    name = re.sub(r"\s+", " ", name).strip()
    return name



def substring_jaro_winkler(needle, haystack, threshold=0.85):
    """
    needle'ın haystack içinde geçip geçmediğini Jaro-Winkler ile kontrol eder.
    Sadece sliding window kullanır - kelime bazlı karşılaştırma yapmaz.
    """
    needle_words = needle.split()
    haystack_words = haystack.split()
    
    if not needle_words or not haystack_words:
        return 0.0

    if len(needle_words) == 1 and len(needle_words[0]) < 5:
        return 0.0
    
    window_size = len(needle_words)
    best_score = 0.0
    
    for i in range(len(haystack_words) - window_size + 1):
        window = " ".join(haystack_words[i:i + window_size])
        score = jaro_winkler_similarity(needle, window)
        if score > best_score:
            best_score = score
    
    return best_score



def match_content(df, content, jaro_threshold=0.88):
    """
    content içinde:
    1. Kod column'undan exact match arayanları bulur
    2. Şirket Ünvanı'ndan Jaro-Winkler similarity ile arayanları bulur
    
    Returns:
        dict: {
            "exact_kod_matches": [...],
            "fuzzy_name_matches": [...]
        }
    """
    content_upper = content.upper()
    
    # --- 1. EXACT KOD MATCH ---
    exact_kod_matches = []
    for _, row in df.iterrows():
        kod = str(row["Kod"]).strip().upper()
        # Tam kelime olarak geçiyor mu? (örn. THYAO, SISE)
        pattern = r'\b' + re.escape(kod) + r'\b'
        if re.search(pattern, content):
            exact_kod_matches.append({
                "Kod": row["Kod"],
                "Şirket Ünvanı": row["Şirket Ünvanı"],
                "match_type": "exact_kod"
            })
    
    # --- 2. FUZZY ŞİRKET ÜNVANI MATCH ---
    fuzzy_name_matches = []
    already_matched_kods = {m["Kod"] for m in exact_kod_matches}
    
    for _, row in df.iterrows():
        if row["Kod"] in already_matched_kods:
            continue  # Zaten exact match var, atla
        
        cleaned_name = clean_company_name(row["Şirket Ünvanı"])
        if not cleaned_name:
            continue
        
        score = substring_jaro_winkler(cleaned_name, content_upper, threshold=jaro_threshold)
        
        if score >= jaro_threshold:
            fuzzy_name_matches.append({
                "Kod": row["Kod"],
                "Şirket Ünvanı": row["Şirket Ünvanı"],
                "cleaned_name": cleaned_name,
                "score": round(score, 4),
                "match_type": "fuzzy_name"
            })
    
    # Score'a göre sırala
    fuzzy_name_matches.sort(key=lambda x: x["score"], reverse=True)

    matched_kods = (
        [m["Kod"] for m in exact_kod_matches] +
        [m["Kod"] for m in fuzzy_name_matches]
    )

    matched_names = (
        [m["Şirket Ünvanı"] for m in exact_kod_matches] +
        [m["Şirket Ünvanı"] for m in fuzzy_name_matches]
    )
    
    # kod → şirket ünvanı eşlemesi
    kod_to_name = {
        m["Kod"]: m["Şirket Ünvanı"]
        for m in exact_kod_matches + fuzzy_name_matches
    }

    return {
        "exact_kod_matches": exact_kod_matches,
        "fuzzy_name_matches": fuzzy_name_matches,
        "matched_kods": matched_kods,
        "matched_names": matched_names,
        "kod_to_name": kod_to_name,
    }


# --- TEST ---
"""
results = match_content(content, df)

print("=== EXACT KOD MATCHES ===")
for m in results["exact_kod_matches"]:
    print(f"  [{m['Kod']}] {m['Şirket Ünvanı']}")

print("\n=== FUZZY NAME MATCHES ===")
for m in results["fuzzy_name_matches"]:
    print(f"  [{m['Kod']}] {m['Şirket Ünvanı']}  →  score: {m['score']}")
"""