from rpyc.utils.server import ThreadedServer
from rpyc.utils.helpers import classpartial

from src.services.nameserver import NameServerService
from src.utils import (
    load_tree
)

def startNameServer(treeJsonFilename, hostname, port):
    tree = load_tree(treeJsonFilename)
    service = classpartial(NameServerService, tree, treeJsonFilename)
    t = ThreadedServer(service, hostname=hostname, port=port)
    t.start()