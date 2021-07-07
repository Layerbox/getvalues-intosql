##############################################################################################################
### Author:         Niklas Eckhardt                                                                          #
### Version:        1.0                                                                                      #
### Stand:          19.06.2021                                                                               #
### Beschreibung:   Holt den Höhenstand und Zeitstempel von Pegelonline.de und schiebt ihn in SQL Datenbank  #
##############################################################################################################

import configparser
import os.path
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

check = os.path.isfile("config.ini")

if check:
### Zeitstempel bilden #######################################################################################
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
##############################################################################################################

### Daten holen von API Pegelonline ##########################################################################
    url = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations/DUISBURG-RUHRORT/W/currentmeasurement.json"
    r = requests.get(url)
    cont = r.json()
    result = cont["value"]
    timestamper = cont["timestamp"]
    parta  = timestamper [11:-6]
    partb  = timestamper [:-15]
    timestamp_unten = '{} {}'.format(partb, parta)
##############################################################################################################

    config = configparser.ConfigParser()
    config.read("config.ini")
    host = config["SQL-LOGIN"]['host']
    port = config["SQL-LOGIN"]['port']
    user = config["SQL-LOGIN"]['user']
    passwd = config["SQL-LOGIN"]['password']
    database = config["SQL-LOGIN"]['database']

### Verbindung zur SQL Datenbank herstellen und überprüfen ###################################################
    def create_connection(host_name, port, user_name, user_password, db_name):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                passwd=passwd,
                database=database
            )
            print("\n", "Verbindung zur MySQL DB geöffnet")
        except Error as e:
            print(f"Der Fehlert '{e}' ist aufgetreten")
        return connection

    connection = create_connection(host, port, user, passwd, database)
##############################################################################################################

### Daten in Tabelle einfügen ################################################################################
    mycursor = connection.cursor()
    sql = """INSERT INTO Rhein_Pegel (VALUE, TIMESTAMP) VALUES (%s, %s)"""
    mycursor.execute(sql, (result, timestamp_unten))
    connection.commit()

    print ("\n", result, "cm in Tabelle eingefügt mit dem Zeitstempel", timestamp_unten)
##############################################################################################################

    if connection.is_connected():
        mycursor.close()
        connection.close()
        print ("\n", "Verbindung zur MySQL DB geschlossen")


else:
    config = configparser.ConfigParser()
    if not config.has_section("SQL-LOGIN"):
        config.add_section("SQL-LOGIN")
        config.set("SQL-LOGIN", "host", "1.1.1.1 #Bitte aendern!")
        config.set("SQL-LOGIN", "port", "1234 #Bitte aendern!")
        config.set("SQL-LOGIN", "user", "benutzer #Bitte aendern!")
        config.set("SQL-LOGIN", "password", "passwort #Bitte aendern!")
        config.set("SQL-LOGIN", "database", "datenbank #Bitte aendern!")

    with open("config.ini", 'w') as configfile:
        config.write(configfile)

    print("Configdatei wurde erstellt, bitte anpassen!\n")
    exit()