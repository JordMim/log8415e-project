from flask import request, Flask
import  pymysql, pymysql.cursors
from sshtunnel import open_tunnel
import os, paramiko, random


def private_key() -> paramiko.RSAKey:
    '''
    read the private key for ssh
    '''

    if not os.path.isfile(f'../ssh_key.pem'):
        return None
    file = open(f'../ssh_key.pem', 'r')
    return paramiko.RSAKey.from_private_key(file)

PRIVATE_KEY = private_key()
MASTER_HOSTNAME = "ip-172-31-91-193.ec2.internal"
MASTER_DB = "clusterDB"
SLAVES = [
    "ip-172-31-83-155.ec2.internal",
    "ip-172-31-91-210.ec2.internal",
    "ip-172-31-89-19.ec2.internal"
]

#Create flask app#
app = Flask('log8415e_project')

@app.route("/ping", methods=["GET"])
def PING():
    return 'pong!', 200   

@app.route("/direct", methods=["POST"])
def direct_route():
    '''
    direct route for proxy
    '''

    data = request.get_json()
    query: str = data['query']
    try:
        run_query(query, mode='direct')
    except Exception as e:
        return str(e), 500
    return {
        data: ""
    }, 200

@app.route("/random", methods=["POST"])
def random_route():
    '''
    random route for proxy
    '''

    data = request.get_json()
    query: str = data['query']
    try:
        run_query(query, mode='random')
    except Exception as e:
        return str(e), 500
    return { 
        data: ""
    }, 200
    
@app.route("/custom", methods=["POST"])
def custom_route():
    '''
    custom route for proxy
    '''

    data = request.get_json()
    query: str = data['query']
    try:
       run_query(query, mode='custom')
    except Exception as e:
        return str(e), 500
    return {
        data:""
    }, 200

def run_query(query:str, mode: str):
    '''
    Executes a MySQL query with a specific mode and parameters
    '''

    # Get a specific node
    try:
        db= get_node(mode)
        if db is None:
            raise Exception('Can\'t get this node')
    except Exception as e:
        raise Exception('Error while getting a node: ' + str(e))
    
    # Connect to a tunnel with openSSH  
    try:
        with open_tunnel(
        (db, 22),
        ssh_username='ubuntu',
        ssh_pkey=PRIVATE_KEY,
        remote_bind_address=(MASTER_HOSTNAME, 3306),
        local_bind_address=('0.0.0.0', 3306)) as tunnel:
            try:
                connect_to_DB()
            except Exception as e:
                raise Exception('Error while connecting to MySQL: ' + str(e))
                
    except Exception as e:
        raise Exception('Error while connecting with openSSH' + str(e))  
        
def connect_to_DB():
    # not implemented yet
    return None

def get_node(mode: str):
    '''
    return a instance node
    '''

    # Direct mode will return master
    if mode == 'direct':
        return MASTER_HOSTNAME, MASTER_DB

    # Random mode will return a random slave
    elif mode == 'random':
        i = random.randint(0, 2)
        return SLAVES[i], 'slave' + str(i+1)
    
    # Custom mode will return the fastest one based on its ping
    # elif mode == 'custom':
        # not implemented yet
    
    # Other modes are not supported
    else:
        return None         



if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=80)