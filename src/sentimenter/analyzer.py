from transformers import pipeline
import os

def prepare_analyzer():

    base_path = os.environ.get(
        'AIRFLOW_HOME',
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
    yerel_yol = os.path.join(base_path, "src", "models", "berturk-financial-sentiment-analysis")

    analyzer = pipeline(
        "sentiment-analysis", 
        model=yerel_yol, 
        tokenizer=yerel_yol, 
        device=-1
    )

    return analyzer