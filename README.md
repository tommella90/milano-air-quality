# How to use

## Download data
You can directly download the data running 
```python download_from_gcloud.py```

Or scrape them and clean them running: 
```python scraping.py``` 

Note tath this will scrape date from 2010, which will take long. 
You can modify the START_DATE inside the code, at the beginning. 

```pytnon clean_data.py``` 
will clean the dataset. 

In both cases, the data folder will be populated. 

## Streamlit app
Available at: 
https://milano-air-quality-x3ehkuzdtuvdyvwz4omep5.streamlit.app/

or run: 
```pipenv shell```
```streamlit run streamlit_app.py```