import pymongo
from py2neo import Graph, Node, Relationship

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["HP"]  # Reemplaza con el nombre de tu base de datos
collection = db["characters"]  # Reemplaza con el nombre de tu colección

# Conexión a Neo4j
graph = Graph("bolt://13.57.206.177:7687", auth=("neo4j", "Lopez6630316664."))  # Reemplaza con tus credenciales y dirección IP

# Extraer los datos de MongoDB
data = list(collection.find())

# Generar nodos y relaciones
nodes = []
houses = {}
wand_cores = {}
ancestries = {}
relationships = []

# Crear nodos y preparar relaciones
print("Creando nodos y preparando relaciones...")
for item in data:
    node = Node("Character",
                id=item['id'],
                name=item['name'],
                species=item['species'],
                gender=item['gender'],
                house=item['house'],
                dateOfBirth=item['dateOfBirth'],
                yearOfBirth=item['yearOfBirth'],
                wizard=item['wizard'],
                ancestry=item['ancestry'],
                eyeColour=item['eyeColour'],
                hairColour=item['hairColour'],
                wand_wood=item['wand']['wood'],
                wand_core=item['wand']['core'],
                wand_length=item['wand']['length'],
                patronus=item['patronus'],
                hogwartsStudent=item['hogwartsStudent'],
                hogwartsStaff=item['hogwartsStaff'],
                actor=item['actor'],
                alive=item['alive'],
                image=item['image'])
    nodes.append(node)

    # Crear o obtener nodos de casa
    if item['house'] not in houses:
        houses[item['house']] = Node('House', name=item['house'])

    # Crear o obtener nodos de wand_core
    if item['wand']['core'] not in wand_cores:
        wand_cores[item['wand']['core']] = Node('Wand_Core', name=item['wand']['core'])

    # Crear o obtener nodos de ancestry
    if item['ancestry'] not in ancestries:
        ancestries[item['ancestry']] = Node('Ancestry', name=item['ancestry'])

    # Preparar relaciones
    relationships.append(Relationship(node, 'BELONGS_TO', houses[item['house']]))
    relationships.append(Relationship(node, 'HAS_WAND_CORE', wand_cores[item['wand']['core']]))
    relationships.append(Relationship(node, 'HAS_ANCESTRY', ancestries[item['ancestry']]))

# Crear nodos y relaciones en la base de datos en lotes
print("Iniciando transacciones en lotes...")
batch_size = 1000  # Define your batch size here
num_of_nodes = len(nodes)
num_of_relationships = len(relationships)

for i in range(0, num_of_nodes, batch_size):
    tx = graph.begin()
    for node in nodes[i : i+batch_size]:
        tx.create(node)
    tx.commit()
    print(f"Creados nodos {i+batch_size} de {num_of_nodes}")

for i in range(0, num_of_relationships, batch_size):
    tx = graph.begin()
    for relationship in relationships[i : i+batch_size]:
        tx.create(relationship)
    tx.commit()
    print(f"Creadas relaciones {i+batch_size} de {num_of_relationships}")

print("¡Todas las transacciones se han completado exitosamente!")
