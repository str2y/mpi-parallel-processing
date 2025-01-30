import pandas as pd
spotify = pd.read_csv('spotify_millsongdata.csv')
texto_col = spotify['text']
clean_texto = texto_col.str.replace(r"[^\w\s']+", ' ', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()
clean_texto.to_csv('letras.csv', index=False, header=['letras'], encoding='utf-8')
artista_col = spotify['artist']
clean_artista = artista_col.str.replace(r"[^\w\s']+", ' ', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()
clean_artista.to_csv('artistas.csv', index=False, header=['artista'], encoding='utf-8')
