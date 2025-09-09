#!/usr/bin/env python3

import csv
import sys
import sqlite3
import numpy as np

con = sqlite3.connect("aa_irc2025.db")
con.row_factory = sqlite3.Row

def prepare_db():
    print("Creazione database...")
    cur = con.cursor()
    cur.execute("CREATE TABLE SCUOLE (ANNOSCOLASTICO NUMERIC, AREAGEOGRAFICA TEXT, REGIONE TEXT, PROVINCIA TEXT, CODICEISTITUTORIFERIMENTO TEXT, CODICESCUOLA TEXT, DENOMINAZIONESCUOLA TEXT, INDIRIZZOSCUOLA TEXT, CAPSCUOLA TEXT, CODICECOMUNESCUOLA TEXT, DESCRIZIONECOMUNE TEXT,  DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA TEXT, INDIRIZZOEMAILSCUOLA TEXT, INDIRIZZOPECSCUOLA TEXT, SITOWEBSCUOLA TEXT, TIPO TEXT)");
    cur.execute("CREATE TABLE LIBRITESTO (CODICESCUOLA TEXT, ANNOCORSO NUMERIC, SEZIONEANNO TEXT, TIPOGRADOSCUOLA TEXT, COMBINAZIONE TEXT, DISCIPLINA TEXT, CODICEISBN TEXT, AUTORI TEXT, TITOLO TEXT, SOTTOTITOLO TEXT, VOLUME TEXT, EDITORE TEXT, PREZZO TEXT, NUOVAADOZ TEXT, DAACQUIST TEXT, CONSIGLIATO TEXT)")
    con.commit()

def load_schools(source, kind):
    print(f"Caricamento dati scuole da {source}")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        for row in csv_reader:
            #print(row)
            row.pop('SEDESCOLASTICA', None) # solo nei file di TrentinoAA e Valle d'Aosta è presente
            row.pop('DENOMINAZIONEISTITUTORIFERIMENTO', None)
            row.pop('DESCRIZIONECARATTERISTICASCUOLA', None)
            row.pop('INDICAZIONESEDEDIRETTIVO', None)
            row.pop('INDICAZIONESEDEOMNICOMPRENSIVO', None)
            row['TIPO'] = kind
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO SCUOLE VALUES(" + "?, "*15 +"?)", data)
    con.commit()
    print(str(line_count) + " linee caricate")

def load_books(source):
    print(f"Caricamento dati adozione libri da {source}")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        for row in csv_reader:
            #print(row)
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO LIBRITESTO VALUES(" + "?, "*15 +"?)", data)
    con.commit()
    print(str(line_count) + " linee caricate")

def remove_subjects():
    print("Eliminazione delle materie curricolari ordinarie...")
    cur = con.cursor()
    con.execute("DELETE FROM LIBRITESTO WHERE DISCIPLINA NOT IN ('ADOZIONE ALTERNATIVA ART. 156 D.L. 297/94', 'RELIGIONE CATTOLICA/ATTIVITA'' ALTERNATIVA');")
    con.commit()
    cur.execute('VACUUM;')
    con.commit()

if __name__ == '__main__':
    prepare_db()
    
    for source in [
        'SCUANAAUTSTAT20252620250901.csv', #pubbliche delle regioni autonome, 2025/26
        'SCUANAGRAFESTAT20252620250901.csv', # pubbliche statali, 2025/26
        ]:
            load_schools(source, 'PUBBLICA')

    for source in [   
            'ALTABRUZZO000020250901.csv',
            'ALTBASILICATA000020250901.csv',
            'ALTCALABRIA000020250901.csv',
            'ALTCAMPANIA000020250901.csv',
            'ALTEMILIAROMAGNA000020250901.csv',
            'ALTFRIULIVENEZIAGIULIA000020250901.csv',
            'ALTLAZIO000020250901.csv',
            'ALTLIGURIA000020250901.csv',
            'ALTLOMBARDIA000020250901.csv',
            'ALTMARCHE000020250901.csv',
            'ALTMOLISE000020250901.csv',
            'ALTPIEMONTE000020250901.csv',
            'ALTPUGLIA000020250901.csv',
            'ALTSARDEGNA000020250901.csv',
            'ALTSICILIA000020250901.csv',
            'ALTTOSCANA000020250901.csv',
            'ALTTRENTINOALTOADIGE000020250901.csv',
            'ALTUMBRIA000020250901.csv',
            'ALTVALLEDAOSTA000020250901.csv',
            'ALTVENETO000020250901.csv',
        ]:
            load_books(source)
            pass

    #remove_subjects();
