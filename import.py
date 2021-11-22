import os

from neo4j import GraphDatabase
import json
import random
import logging
import uuid

# default parameters
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "neo4j")
DATABASE = "neo4j"


class Converter(object):
    """Convert database schema to Hume Schema import file format """

    # cypher to Hume data types mapping
    TYPES_MAPPING = {'String': 'STRING', 'Long': 'NUMBER', 'Double': "DOUBLE", "StringArray": "STRING",
                     'Date': 'DATE', 'Point': 'STRING', 'Boolean': 'BOOLEAN', }

    def __init__(self, uri, auth, database="neo4j"):
        logging.basicConfig(level=os.environ.get("DEUG_LEVEL", "INFO"))
        self.logger = logging.getLogger()

        # connect to database
        self.logger.info(f"connecting to {uri}/{database}")
        self.driver = GraphDatabase.driver(uri, auth=auth, database=database)
        self.session = self.driver.session()

        # class id to internal uuid mapping
        self._class_uuid = {}

        # fetch classes and relationships
        classes, relationships = self.populate_schema()
        self.logger.info(f"got {len(classes)} classes and {len(relationships)} relationships")

        # add attriute to classes
        self.collect_attributes(classes)

        self.schema = {'classes': list(classes.values()),
                       'relationships': list(relationships.values())}

    def populate_schema(self):
        """use db.schema.visualization() to grab information about classes and relationship amongst classess
            :returns found classes, relationships
        """
        query = "CALL db.schema.visualization()"
        schema = list(self.session.run(query))[0]
        classes = {node["name"]: self._make_class(node) for node in schema['nodes']}
        relationships = {rel.type: self._make_rel(rel) for rel in schema['relationships']}
        return classes, relationships

    def collect_attributes(self, classes):
        """use db.schema.nodeTypeProperties() to attach attribute information to the classes
        """
        query = "CALL db.schema.nodeTypeProperties()"
        node_type_properties = list(self.session.run(query))

        attributes_map = {}

        for node in node_type_properties:
            labels = node['nodeLabels']
            property_name = node['propertyName']
            node_type = node['nodeType']

            # multiple labels not supported
            if len(labels) == 1:
                label = labels[0]
                humeType = self._to_hume_type(node)
                attributes_map.setdefault(label, []).append({'label': property_name,
                                                             'type': humeType})
            else:
                self.logger.warning(f"got multiple labels for {node_type}  ({labels})")

        for (key, attributes) in attributes_map.items():
            classes[key]['attributes'] = list(attributes)

    def _make_class(self, node):
        """create a class object out of the node. It updates the class.id to uuid relation"""
        class_uuid = str(uuid.uuid4())
        self._class_uuid[node.id] = class_uuid
        return {'label': node['name'],
                'canvasPosition': self._random_canvas_position(),
                'icon': 'mdi-circle-outline',
                'color': '#aaa',
                'uuid': class_uuid}

    def _make_rel(self, relation):
        """create a relation object out of the relation entry."""
        node_from = relation.nodes[0]
        node_to = relation.nodes[1]
        return {'uuid': str(uuid.uuid4()),
                'start': self._class_uuid[node_from.id],
                'startLabel': node_from['name'],
                'endLabel': node_to['name'],
                'endId': self._class_uuid[node_to.id],
                'label': relation.type}

    def _to_hume_type(self, node):
        """extract the HUME type for the node"""
        nodeType = node['nodeType']
        property_types = node['propertyTypes']
        property_type = property_types[0]
        if len(property_types) > 1:
            self.logger.warning(f"multiple property types for {nodeType} ({property_types})")
        if property_type not in self.TYPES_MAPPING:
            self.logger.warning(f"unsupported type {property_type} for {nodeType} falling back to STRING")
        return self.TYPES_MAPPING.get(property_type, "STRING")

    @staticmethod
    def _random_canvas_position():
        """return a random canvas position"""
        return {'x': random.randint(100, 1200), 'y': random.randint(50, 800)}


if __name__ == "__main__":
    converter = Converter(URI, AUTH, DATABASE)
    with open('schema-generated.json', 'w') as fp:
        json.dump(converter.schema, fp)