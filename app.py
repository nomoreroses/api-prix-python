import os
from flask import Flask, request, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

# --- CONFIGURATION INITIALE & CHARGEMENT DE LA BASE ---
DB_PATH = "database.parquet"
df = pd.DataFrame() # DataFrame vide par défaut

print("⏳ Tentative de chargement de la base de données Parquet...")
try:
    if os.path.exists(DB_PATH):
        df = pd.read_parquet(DB_PATH)
        print(f"✅ Base chargée : {len(df)} véhicules en mémoire.")
    else:
        print("❌ Erreur : Fichier database.parquet introuvable. L'estimation sera impossible.")
except Exception as e:
    print(f"❌ Erreur critique lors du chargement de la base : {e}")
    df = pd.DataFrame()

# Fonction pour trouver la bonne colonne (s'appelle 'modele', 'model', 'libelle', etc.)
def find_column(df, names):
    """Cherche la première colonne existante parmi une liste de noms."""
    for name in names:
        if name in df.columns:
            return name
    return None

# --- ROUTES ---

@app.route('/', methods=['GET'])
def home():
    return f"API Python EstimyCar Lite en ligne. Base chargée : {len(df)} véhicules."

@app.route('/estimer', methods=['POST'])
def estimer():
    # Sécurité: Si la base n'est pas chargée, on quitte.
    if df.empty:
        return jsonify({
            'prix_estime': None, 
            'msg': 'Erreur interne: Base de données non disponible.',
            'erreur': 'BaseDeDonneesManquante'
        }), 500
        
    try:
        data = request.json
        # On récupère les inputs, avec des valeurs par défaut sécurisées
        modele_input = str(data.get('modele', '')).lower()
        annee_input = int(data.get('annee', 2015))
        km_input = int(data.get('km', 100000))

        # --- DÉTECTION DES NOMS DE COLONNES (Le correctif pour l'erreur 'km') ---
        COL_MODELE = find_column(df, ['modele', 'model', 'libelle'])
        COL_ANNEE = find_column(df, ['annee', 'year'])
        COL_KM = find_column(df, ['km', 'mileage', 'kilometrage'])
        COL_PRIX = find_column(df, ['prix', 'price'])
        
        # Vérification finale des colonnes pour éviter un crash plus tard
        if not all([COL_MODELE, COL_ANNEE, COL_KM, COL_PRIX]):
            return jsonify({
                'prix_estime': None, 
                'msg': 'Erreur interne: Colonnes de base introuvables (Prix, Année ou KM).',
                'erreur': 'ColonnesManquantes'
            }), 500

        # --- PRÉ-TRAITEMENT ET FILTRAGE ---
        
        # On crée une colonne de recherche en minuscule pour le modèle
        df['temp_search'] = df[COL_MODELE].astype(str).str.lower()
        
        # 1. Filtre par Modèle (recherche floue)
        candidates = df[df['temp_search'].str.contains(modele_input, na=False, regex=False)].copy()
        
        if len(candidates) < 5:
            return jsonify({'prix_estime': None, 'msg': 'Pas assez de données pour ce modèle spécifique.'})

        # 2. Filtre par Année (+/- 2 ans)
        candidates = candidates[
            (candidates[COL_ANNEE] >= annee_input - 2) & 
            (candidates[COL_ANNEE] <= annee_input + 2)
        ]

        # 3. Filtre par Kilométrage (+/- 30 000 km)
        # On remplace les NaN de la colonne KM par une valeur très haute pour qu'ils soient exclus par le filtre
        candidates = candidates[
            (candidates[COL_KM].fillna(999999) >= km_input - 30000) & 
            (candidates[COL_KM].fillna(999999) <= km_input + 30000)
        ]

        if len(candidates) < 3:
            return jsonify({'prix_estime': None, 'msg': 'Pas assez de véhicules similaires (année/km) pour estimer.'})

        # --- CALCUL FINAL ---
        
        # SÉCURITÉ : On s'assure que la colonne prix ne contient pas de NaN qui ferait planter median()
        final_prices = candidates[COL_PRIX].dropna()
        
        prix_estime = int(final_prices.median())
        nb_similaires = len(final_prices)

        return jsonify({
            'prix_estime': prix_estime,
            'confiance': f'Basé sur {nb_similaires} annonces réelles',
            'details': {
                'min': int(final_prices.min()),
                'max': int(final_prices.max())
            }
        })

    except Exception as e:
        print(f"❌ Erreur calcul fatale dans /estimer : {e}")
        # Renvoi du statut 500 pour indiquer que le crash est interne
        return jsonify({'prix_estime': None, 'erreur': f'Erreur interne du serveur Python: {e}'}), 500

if __name__ == '__main__':
    # Le port 10000 est souvent utilisé dans les configurations locales Render
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 10000))