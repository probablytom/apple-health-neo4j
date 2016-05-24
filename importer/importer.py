# A little library for parsing Apple Health data and feeding it into a Neo4J database

from xml.dom import minidom

import time
from neo4j.v1 import GraphDatabase, basic_auth

def import_from_xml(csv_filename, database_url = "bolt://localhost", neo_user = "neo4j",neo_pass = "fivejumpingj"):

    # Log into the database
    driver = GraphDatabase.driver(database_url, auth=basic_auth(neo_user, neo_pass))

    # Parse CSV
    print "Parsing xml file into a DOM..."
    data = minidom.parse(csv_filename)
    sources = []

    print "Onto the data entry..."

    # Run through the different types of entry in a V3 Apple Health export and import them
    #  ...creating nodes and relationships to input sources.
    for entry_type in ["Record", "Workout", "WorkoutEntry", "Correlation"]:
        entry_collection = data.getElementsByTagName(entry_type)
        record_number = 0


        for entry in entry_collection:
            record_number += 1
            node_name = entry_type + "_" + str(record_number)
            cypher_command = "create (" + node_name + ":" + entry_type + " { entry_name: \'" + node_name + "\', "
            source = ""

            for attr in entry.attributes.keys():
                if attr is not u'type':
                    if str(attr) == "sourceName":

                        source = str(entry.attributes[attr].value)

                        if entry.attributes[attr].value not in sources:
                            sources.append(entry.attributes[attr].value)

                            # Create the input source as a :Source node
                            session = driver.session()
                            session.run("create (" + source + ":Source {sourceName: \"" + source + "\"}) ")
                            session.close()
                            print "create (" + source + ":Source {sourceName: \"" + source + "\"})"

                    try:
                        if type(entry.attributes[attr].value) is unicode:
                            val = str(entry.attributes[attr].value)
                        else:
                            val = str(entry.attributes[attr].value())
                        cypher_command += str(attr) + ": \"" + val + "\", "

                    except TypeError:
                        print type(entry.attributes[attr].value), "is a format in the xml that I can't process! Sorry. Bombing out now, but be aware that changes to the database have been commited!"
                        print "here's the string version of the entry I failed on:"
                        print str(entry)
                        print "\nGood luck!\n"
                        exit(0)

            cypher_command = cypher_command[:-2] + "})"
            print cypher_command
            session = driver.session()
            session.run(cypher_command)
            session.close()

            cypher_command = "MATCH (source:Source {sourceName: \"" + source + "\"}), (entry {entry_name: \"" + node_name + "\"}) create (entry)-[:ENTERED_THROUGH]->(source) "
            print cypher_command
            session = driver.session()
            session.run(cypher_command)
            session.close()
            if record_number == 1000:
                exit()

