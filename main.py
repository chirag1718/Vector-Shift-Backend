from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import json
from collections import defaultdict, deque

# grab settings from .env file
load_dotenv()

app = FastAPI()

# let the frontend talk to us
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("FRONTEND_URL")], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

def is_dag(nodes, edges):
    # empty graph is always a DAG
    if not nodes:
        return True
    
    # nodes without connections are fine
    if not edges:
        return True
    
    # track connections and incoming edges
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    # count incoming edges for each node
    for node in nodes:
        if not isinstance(node, dict) or 'id' not in node:
            return False  # bad node format
        node_id = node['id']
        in_degree[node_id] = 0
    
    # build the graph connections
    for edge in edges:
        if not isinstance(edge, dict) or 'source' not in edge or 'target' not in edge:
            return False  # bad edge format
        
        source = edge['source']
        target = edge['target']
        
        # make sure both nodes actually exist
        if source not in in_degree or target not in in_degree:
            return False  # edge points to missing node
        
        graph[source].append(target)
        in_degree[target] += 1
    
    # start with nodes that have no incoming edges
    queue = deque([node for node in in_degree if in_degree[node] == 0])
    processed = 0
    
    # remove nodes one by one
    while queue:
        current = queue.popleft()
        processed += 1
        
        # update counts for connected nodes
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # if we processed everything, no cycles found
    return processed == len(nodes)

@app.post('/pipelines/parse')
def parse_pipeline(nodes: str = Form(...), edges: str = Form(...)):
    """
    Takes pipeline data and figures out how many nodes/edges there are,
    plus whether it's a valid DAG. Handles weird cases like empty data.
    """
    try:
        # convert the JSON strings back to actual data
        nodes_data = json.loads(nodes)
        edges_data = json.loads(edges)
        
        # list check
        if not isinstance(nodes_data, list):
            return {'error': 'Nodes data must be a list'}
        if not isinstance(edges_data, list):
            return {'error': 'Edges data must be a list'}
        
        # count nodes and edges
        num_nodes = len(nodes_data)
        num_edges = len(edges_data)
        
        if num_nodes == 0 and num_edges == 0:
            return {
                'num_nodes': 0,
                'num_edges': 0,
                'is_dag': True, 
                'message': 'Empty pipeline - no nodes or edges'
            }
        
        if num_nodes == 0 and num_edges > 0:
            return {
                'num_nodes': 0,
                'num_edges': num_edges,
                'is_dag': False, 
                'message': 'Invalid pipeline - edges exist without nodes'
            }
        
        # check for cycles
        is_dag_result = is_dag(nodes_data, edges_data)
        
        # build the response
        response = {
            'num_nodes': num_nodes,
            'num_edges': num_edges,
            'is_dag': is_dag_result
        }
        
        # add context 
        if num_nodes > 0 and num_edges == 0:
            response['message'] = 'Pipeline contains only isolated nodes'
        elif not is_dag_result:
            response['message'] = 'Pipeline contains cycles and is not a valid DAG'
        else:
            response['message'] = 'Valid pipeline structure'
        
        return response
    
    except json.JSONDecodeError as e:
        return {'error': f'Invalid JSON format: {str(e)}'}
    except Exception as e:
        return {'error': f'Processing error: {str(e)}'}
