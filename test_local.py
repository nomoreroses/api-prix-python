import pandas as pd
import os

# Chemin vers le fichier que tu as listé dans ton ls -R
DB_PATH = "data/database.parquet"

def scanner_prix():
    print("------------------------------------------------")
    print(f"📂 Recherche du fichier : {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print("❌ ERREUR : Le fichier database.parquet est introuvable !")
        print("Vérifie que tu es bien dans le dossier EstimyCar-main")
        return

    print("⏳ Chargement de la base de données (ça peut prendre quelques secondes)...")
    
    try:
        # On charge le fichier
        df = pd.read_parquet(DB_PATH)
        print(f"✅ Base chargée avec succès !")
        print(f"📊 Nombre total de véhicules en mémoire : {len(df)}")
        print("------------------------------------------------")
        
        # On affiche les colonnes pour être sûr des noms
        print(f"ℹ️ Colonnes détectées : {list(df.columns)}")
        print("------------------------------------------------")

        # Boucle de test infini
        while True:
            print("\n--- NOUVELLE ESTIMATION ---")
            modele_input = input("🚗 Modèle (ex: clio, golf, 308) ou 'q' pour quitter : ").strip().lower()
            if modele_input == 'q': break
            
            try:
                annee_input = int(input("📅 Année (ex: 2015) : "))
                km_input = int(input("road Kilométrage (ex: 100000) : "))
            except ValueError:
                print("❌ Erreur : Il faut entrer des nombres pour l'année et le km.")
                continue

            # --- LA LOGIQUE DE FILTRAGE ---
            
            # 1. On cherche la colonne qui contient le modèle (souvent 'modele' ou 'model' ou 'libelle')
            # On essaie de deviner le nom de la colonne modèle
            col_modele = None
            for col in ['modele', 'model', 'libelle', 'designation', 'version']:
                if col in df.columns:
                    col_modele = col
                    break
            
            if not col_modele:
                # Si on ne trouve pas, on prend la 2ème colonne au pif (souvent la marque/modèle)
                col_modele = df.columns[1] 

            # Conversion en minuscule pour la recherche
            df['temp_search'] = df[col_modele].astype(str).str.lower()
            
            # FILTRE 1 : Le modèle
            candidates = df[df['temp_search'].str.contains(modele_input, na=False)]
            print(f"🔎 {len(candidates)} annonces trouvées pour '{modele_input}'")

            if len(candidates) == 0:
                print("❌ Aucun véhicule trouvé avec ce nom.")
                continue

            # FILTRE 2 : Année (+/- 2 ans)
            # On cherche la colonne année
            col_annee = 'annee' if 'annee' in df.columns else 'year'
            candidates = candidates[
                (candidates[col_annee] >= annee_input - 2) & 
                (candidates[col_annee] <= annee_input + 2)
            ]

            # FILTRE 3 : KM (+/- 30 000 km)
            # On cherche la colonne km
            col_km = 'km' if 'km' in df.columns else 'mileage'
            if 'kilometrage' in df.columns: col_km = 'kilometrage'
            
            candidates = candidates[
                (candidates[col_km] >= km_input - 30000) & 
                (candidates[col_km] <= km_input + 30000)
            ]

            print(f"🎯 {len(candidates)} annonces similaires après filtrage (Année/Km)")

            if len(candidates) > 0:
                # On cherche la colonne PRIX
                col_prix = 'prix' if 'prix' in df.columns else 'price'
                
                prix_moyen = int(candidates[col_prix].mean())
                prix_median = int(candidates[col_prix].median())
                prix_min = int(candidates[col_prix].min())
                prix_max = int(candidates[col_prix].max())

                print(f"\n💰 ESTIMATION RÉSULTAT :")
                print(f"   ➡️ Prix Médian (le plus fiable) : {prix_median} €")
                print(f"   ➡️ Moyenne : {prix_moyen} €")
                print(f"   ➡️ Fourchette : de {prix_min} € à {prix_max} €")
            else:
                print("⚠️ Pas assez de données précises pour estimer.")

    except Exception as e:
        print(f"❌ CRASH DU SCRIPT : {e}")

scanner_prix()