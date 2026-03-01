import re

def sentimenter(analyzer, content):
    weights = {
        "LABEL_2": 1,  # Pozitif
        "LABEL_1": 0,  # Nötr
        "LABEL_0": -1  # Negatif
    }

    sentences = re.split(r'(?<![0-9])\.(?![0-9])|\n', content)
    content_array = [s.strip() for s in sentences if s and len(s.strip()) > 2]

    total_score = 0
    sentence_count = 0

    for line in content_array:
        clean_line = line.strip()
        if len(clean_line) > 15: # Kısa parçaları analiz dışı bırakıyoruz
            prediction = analyzer(clean_line)[0]
            label = prediction['label']
            conf_score = prediction['score']
            
            # Ağırlıklı puan hesapla: Katsayı * Güven
            current_puan = weights.get(label, 0) * conf_score

            total_score += current_puan

            if weights.get(label, 0) != 0:
                sentence_count += 1

    # Ortalama Skoru Hesapla
    if sentence_count > 0:
        final_sentiment_score = total_score / sentence_count
    else:
        final_sentiment_score = 0

    result = 0
    if final_sentiment_score > 0.15:
        result = 2
        overall_sentiment = "🟢 GÜÇLÜ POZİTİF"
    elif final_sentiment_score > 0.05:
        result = 1
        overall_sentiment = "🌿 HAFİF POZİTİF"
    elif final_sentiment_score < -0.15:
        result = -2
        overall_sentiment = "🔴 GÜÇLÜ NEGATİF"
    elif final_sentiment_score < -0.05:
        result = -1
        overall_sentiment = "🍂 HAFİF NEGATİF"
    else:
        result = 0
        overall_sentiment = "⚪ NÖTR / BELİRSİZ"
    
    return result, final_sentiment_score