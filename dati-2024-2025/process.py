#!/usr/bin/env python3

import csv
import sys
import sqlite3
import numpy as np

con = sqlite3.connect("aa_irc2024.db")
con.row_factory = sqlite3.Row

def prepare_db():
    print("Creazione database...")
    cur = con.cursor()
    cur.execute("CREATE TABLE STUDENTI (ANNOSCOLASTICO NUMERIC, CODICESCUOLA TEXT, NUMEROSTUDENTI NUMERIC, STUDENTIIRC NUMERIC, DATODUBBIO TEXT)")
    cur.execute("CREATE TABLE SCUOLE (ANNOSCOLASTICO NUMERIC, AREAGEOGRAFICA TEXT, REGIONE TEXT, PROVINCIA TEXT,  CODICEISTITUTORIFERIMENTO TEXT, CODICESCUOLA TEXT, DENOMINAZIONESCUOLA TEXT, INDIRIZZOSCUOLA TEXT, CAPSCUOLA TEXT, CODICECOMUNESCUOLA TEXT, DESCRIZIONECOMUNE TEXT,  DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA TEXT, INDIRIZZOEMAILSCUOLA TEXT, INDIRIZZOPECSCUOLA TEXT, SITOWEBSCUOLA TEXT, TIPO TEXT)");
    cur.execute("CREATE VIEW DATIRIASSUNTIVI AS SELECT STUDENTI.ANNOSCOLASTICO, STUDENTI.CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, NUMEROSTUDENTI - STUDENTIIRC AS STUDENTINONAVV, AREAGEOGRAFICA, REGIONE, PROVINCIA, CAPSCUOLA, CODICECOMUNESCUOLA, DESCRIZIONECOMUNE, DENOMINAZIONESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, TIPO, DATODUBBIO FROM STUDENTI LEFT JOIN SCUOLE ON STUDENTI.CODICESCUOLA = SCUOLE.CODICESCUOLA AND STUDENTI.ANNOSCOLASTICO = SCUOLE.ANNOSCOLASTICO")
    cur.execute("CREATE VIEW RAPPORTODATIMANCANTI AS SELECT 'SÌ' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'NO' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'TOTALE' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI GROUP BY ANNOSCOLASTICO ORDER BY ANNOSCOLASTICO")
    cur.execute("CREATE TABLE LIBRITESTO (CODICESCUOLA TEXT, ANNOCORSO NUMERIC, SEZIONEANNO TEXT, TIPOGRADOSCUOLA TEXT, COMBINAZIONE TEXT, DISCIPLINA TEXT, CODICEISBN TEXT, AUTORI TEXT, TITOLO TEXT, SOTTOTITOLO TEXT, VOLUME TEXT, EDITORE TEXT, PREZZO TEXT, NUOVAADOZ TEXT, DAACQUIST TEXT, CONSIGLIATO TEXT)")
    cur.execute("CREATE VIEW CLASSIPERSCUOLA AS SELECT MAX(CLASSI) AS NUMEROCLASSI, CODICESCUOLA FROM ADOZIONIPRIMARIE GROUP BY CODICESCUOLA")
    cur.execute("CREATE VIEW ADOZIONIAAIRC AS SELECT COUNT(*) AS CLASSI, TITOLO, SCUOLE.CODICESCUOLA, SCUOLE.CODICEISTITUTORIFERIMENTO, SCUOLE.DENOMINAZIONESCUOLA, SCUOLE.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, SCUOLE.DESCRIZIONECOMUNE, PROVINCIA, REGIONE  FROM LIBRITESTO LEFT JOIN SCUOLE ON LIBRITESTO.CODICESCUOLA = SCUOLE.CODICESCUOLA AND SCUOLE.ANNOSCOLASTICO = 202425 WHERE CODICEISBN IN ('9788809971035', '9788809971042', '9788842632900', '9788842632917', '9788842633150', '9788842633167', '9788846830173', '9788846830180', '9788846844910', '9788846844927', '9788847239838', '9788847239845') GROUP BY TITOLO, SCUOLE.CODICESCUOLA, SCUOLE.DENOMINAZIONESCUOLA, SCUOLE.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, SCUOLE.DESCRIZIONECOMUNE, REGIONE ORDER BY CLASSI DESC")
    cur.execute("CREATE VIEW ADOZIONIAAIRC_CON_ISTITUTO AS SELECT * FROM ADOZIONIAAIRC LEFT JOIN SCUOLE ON ADOZIONIAAIRC.CODICEISTITUTORIFERIMENTO = SCUOLE.CODICESCUOLA")
    cur.execute("CREATE VIEW ADOZIONIPRIMARIE AS SELECT COUNT(*) AS CLASSI, TITOLO, SCUOLE.CODICESCUOLA, SCUOLE.DENOMINAZIONESCUOLA, SCUOLE.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, SCUOLE.DESCRIZIONECOMUNE, PROVINCIA, REGIONE  FROM LIBRITESTO LEFT JOIN SCUOLE ON LIBRITESTO.CODICESCUOLA = SCUOLE.CODICESCUOLA  AND SCUOLE.ANNOSCOLASTICO = 202425 WHERE  DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA='SCUOLA PRIMARIA' GROUP BY TITOLO, SCUOLE.CODICESCUOLA, SCUOLE.DENOMINAZIONESCUOLA, SCUOLE.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, SCUOLE.DESCRIZIONECOMUNE, REGIONE ORDER BY CLASSI DESC")
    con.commit()

def load_students(year, source):
    print(f"Caricamento dati studenti da {source} (anno {year})")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        skip_count = 0
        skipped_students = 0
        students = 0
        for row in csv_reader:
            skip = None
            #print(row)
            if (row['NUMEROSTUDENTI']=='<=3'):
                skip_count+=1
                skip='NUMEROSTUDENTI<=3'
                row['NUMEROSTUDENTI']=3
            elif (row['STUDENTIIRC']=='<=3'):
                #row['STUDENTIIRC']=3
                skip_count+=1
                skipped_students +=int(row['NUMEROSTUDENTI'])
                skip='STUDENTIIRC<=3'
                row['STUDENTIIRC']=3
            '''
            if (skip):
                continue
            '''
            row['ANNOSCOLASTICO'] = year
            row['DATODUBBIO'] = skip
            data.append(tuple(row.values()))
            line_count += 1
            students += int(row['NUMEROSTUDENTI'])
    cur.executemany("INSERT INTO STUDENTI(CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, ANNOSCOLASTICO, DATODUBBIO) VALUES(" + "?, "*4 + "?)", data)
    con.commit()
    print(f"{line_count} righe caricate, {skip_count} righe saltate")
    print(f"{students} studenti presi in carico, {skipped_students} esclusi per incoerenza dati")

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
            
            if (kind=='PARITARIA'):
                updated_row = {}
                for key in row:
                    updated_row[key] = row[key]
                    if key == 'PROVINCIA':
                        updated_row['CODICEISTITUTORIFERIMENTO'] = '§'
                row = updated_row
                
            row['TIPO'] = kind
            
            data.append(tuple(row.values()))
            line_count += 1
            
            #if (line_count == 20):
                #break
            
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

def find_anomalies(threshold):
    print("Ricerca di anomalie...")
    cur = con.cursor()
    
    count = 0
    
    totalNumberOfStudents  = 0
    totalNumberOfSchools = 0
    flaggedData = 0
    flaggedSchools = 0
    
    cur.execute("SELECT DISTINCT CODICESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA FROM SCUOLE WHERE ANNOSCOLASTICO IN (202122, 202223) ORDER BY CODICESCUOLA;")

    while True:
        data = cur.fetchone()
        if data == None:
            break
        schoolcode = data['CODICESCUOLA']
        schooltype = data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']
        schoolyears = 5
        if ('primo grado' in schooltype or 'infanzia' in schooltype):
            schoolyears = 3
        count += 1
        #print("--------")
        print(f"{schoolcode} - {schooltype}, {schoolyears}")
        subcur = con.cursor()
        subcur.execute("SELECT * FROM STUDENTI WHERE STUDENTI.CODICESCUOLA = ? AND DATODUBBIO IS NULL ORDER BY ANNOSCOLASTICO;", (schoolcode, ))
        rows = subcur.fetchall()
        if (len(rows) == 0):
            continue

        percentages = []
        students = []
        rstudents = []
        years = []

        flagged = False
        for row in rows:
            for key in row.keys():
                #print(f"{key}: {row[key]}")
                pass
            if int(row['NUMEROSTUDENTI'])>0:
                percentages.append(row['STUDENTIIRC']/row['NUMEROSTUDENTI'])
                years.append(row['ANNOSCOLASTICO'])
                students.append(row['NUMEROSTUDENTI'])
                rstudents.append(row['STUDENTIIRC'])
                #print("----")

        print(percentages)
        print(years)
        print(students)
        print(rstudents)
        for i in range(0, len(percentages)-1):
            #variation = (percentages[i+1]-percentages[i])/percentages[i]
            variation = max(abs((percentages[i+1]-percentages[i])/percentages[i]), abs((percentages[i]-percentages[i+1])/percentages[i+1]))
            if (variation > threshold):
                # 1/schoolyears
                # print(f"({percentages[i+1]}-{percentages[i]})/{percentages[i]}")
                # print(f"{rstudents[i+1]}/{students[i+1]}")
                con.cursor().execute("UPDATE STUDENTI SET DATODUBBIO = 'INCOERENZE TEMPORALI' WHERE CODICESCUOLA = ? AND ANNOSCOLASTICO = ? ", (schoolcode, years[i+1]))
                print(f"Flagged: {schoolcode} - {schooltype}, {schoolyears}")
                flaggedData += 1
                flagged = True

        totalNumberOfSchools +=1
        if flagged:
            flaggedSchools +=1
            print(f"Flagged: {schoolcode} - {schooltype}, {schoolyears}")
            '''
            if (len(percentages)>0):
                print(percentages, end="  --- ")
                arr = np.array(percentages)
                avg = np.sum(arr) / len(percentages)
                print(avg)
            '''
            report = []
            for i in range(0, len(students)):
                report.append(f"{years[i]}: {rstudents[i]}/{students[i]}")
            print("\n".join(report))
            
            con.cursor().execute("UPDATE STUDENTI SET DATODUBBIO = 'INCOERENZE TEMPORALI' WHERE CODICESCUOLA = ?", (schoolcode,))
    
    print(f"Schools: {totalNumberOfSchools}")
    print(f"Flagged: {flaggedSchools}")
    
    #cur.execute("INSERT INTO VALORIMEDIPERSCUOLA (CODICESCUOLA, STUDENTIIRC) SELECT CODICESCUOLA, AVG(STUDENTIIRC) AS STUDENTIIRC FROM STUDENTI GROUP BY CODICESCUOLA")
    con.commit()

def remove_subjects():
    print("Eliminazione delle materie curricolari ordinarie...")
    cur = con.cursor()
    con.execute("DELETE FROM LIBRITESTO WHERE DISCIPLINA NOT IN ('ADOZIONE ALTERNATIVA ART. 156 D.L. 297/94 ', 'IRC O ATTIVITA ALTERNATIVE');")
    
    con.commit()
    cur.execute('VACUUM;')
    con.commit()

def fix_errors():
    print("Correzione dei dati ministeriali...")
    cur = con.cursor()
    con.execute("UPDATE LIBRITESTO SET CODICEISBN = '9788842632900' WHERE CODICEISBN = '9788867227990';")
    con.commit()

if __name__ == '__main__':
    prepare_db()
    for source in [
        'SCUANAAUTSTAT20242520240901.csv',
        'SCUANAGRAFESTAT20242520240901.csv',
        'INTEGRAZIONEALTOADIGE202223.csv'
        ]:
            load_schools(source, 'PUBBLICA')

    for source in [
        'SCUANAAUTPAR20242520240901.csv',
        'SCUANAGRAFEPAR20242520240901.csv'
        ]:
            load_schools(source, 'PARITARIA')
    for source in [
        'ALTABRUZZO000020240912.csv',
        'ALTBASILICATA000020240912.csv',
        'ALTCALABRIA000020240912.csv',
        'ALTCAMPANIA000020240912.csv',
        'ALTEMILIAROMAGNA000020240912.csv',
        'ALTFRIULIVENEZIAGIULIA000020240912.csv',
        'ALTLAZIO000020240912.csv',
        'ALTLIGURIA000020240912.csv',
        'ALTLOMBARDIA000020240912.csv',
        'ALTMARCHE000020240912.csv',
        'ALTMOLISE000020240912.csv',
        'ALTPIEMONTE000020240912.csv',
        'ALTPUGLIA000020240912.csv',
        'ALTSARDEGNA000020240912.csv',
        'ALTSICILIA000020240912.csv',
        'ALTTOSCANA000020240912.csv',
        'ALTTRENTINOALTOADIGE000020240912.csv',
        'ALTUMBRIA000020240912.csv',
        'ALTVALLEDAOSTA000020240912.csv',
        'ALTVENETO000020240912.csv',

        ]:
            load_books(source)
    
    fix_errors()
    #remove_subjects();
    #find_anomalies(.75)

