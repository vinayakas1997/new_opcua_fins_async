import os
import json
from opcua import Client, ua
import numpy as np
from typing import Any
import logging
import logging.config

class OpcuaAutoNodeMapper:
    """
    A class to automatically map OPC UA nodes to JSON and provide read/write access.
    This class browses the OPC UA server, saves node information to a JSON file,
    Keep in mind : the default file name is "nodes.json" in the current directory.
    can be changed by passing a different json_path parameter.
    In future...
    the nodes information id changed then set parameter reload = True 
    """
    def __init__(self, client: Client, json_path="opcua_json_files/nodes.json",reload=False,console_print=False):
        self.client = client
        self.json_path = json_path
        self.node_map = {}
        self.logger = logging.getLogger("OpcuaAutoNodeMapper")
        self.logger.setLevel(logging.INFO)
        self.console_print = console_print
        self._initialize_node_map(reload)
        
        if os.path.exists(json_path):
            self._load_nodes_from_json()
        else:
            self.logger.info(f"opcua server nodes JSON file {self.json_path} not found. Browsing server...")
            self._browse_and_save_nodes()

    def _initialize_node_map(self, reload=False):
        if reload:
            self.logger.info(f"Reloading server nodes map to JSON file")
            self.node_map = {}
            self._browse_and_save_nodes()
            self.logger.info(f" Node map reloaded successfully ")
            return
    
    def _load_nodes_from_json(self):
        with open(self.json_path) as f:
            self.node_map = json.load(f)
        self.logger.info(f"Loaded {len(self.node_map)} nodes from JSON file {self.json_path}")

    def _browse_and_save_nodes(self):
        # print("[INFO] JSON not found. Browsing server...")
        objects_node = self.client.get_objects_node()
        self.node_map = {}
        self._recursive_browse(objects_node)
        with open(self.json_path, "w") as f:
            json.dump(self.node_map, f, indent=4)
        self.logger.info(f"Saved {len(self.node_map)} nodes to {self.json_path}")

    def _recursive_browse(self, node):
        try:
            for child in node.get_children():
                try:
                    browse_name = child.get_browse_name().Name
                    # node_id_str = str(child.nodeid)
                    node_id_str = child.nodeid.to_string()
                    node_class = child.get_node_class()

                    if node_class == ua.NodeClass.Variable:
                        # Cache the node ID and its data type together
                        data_type_val = child.get_data_type_as_variant_type().value
                        self.node_map[browse_name] = {
                            "node_id": node_id_str,
                            "data_type": data_type_val}

                    self._recursive_browse(child)

                except Exception as e:
                    self.logger.warning(f"Skipping node: \n{e}")
        except Exception as e:
            self.logger.error(f"Cannot browse the node tree: \n{e}")

    def _cast_to_type(self, value, variant_type):
        if variant_type == ua.VariantType.Int16:
            return np.int16(value)
        elif variant_type == ua.VariantType.Int32:
            return np.int32(value)
        elif variant_type == ua.VariantType.Int64:
            return np.int64(value)
        elif variant_type == ua.VariantType.UInt16:
            return np.uint16(value)
        elif variant_type == ua.VariantType.UInt32:
            return np.uint32(value)
        elif variant_type == ua.VariantType.UInt64:
            return np.uint64(value)
        elif variant_type == ua.VariantType.Float:
            return float(value)
        elif variant_type == ua.VariantType.Double:
            return float(value)
        elif variant_type == ua.VariantType.Boolean:
            return bool(value)
        elif variant_type == ua.VariantType.String:
            return str(value)
        else:
            # print(f"[WARN] No cast rule for {variant_type.name}, using raw value")
            return value
    
    def read(self, name):
        node_info = self.node_map.get(name)
        if not node_info:
            self.logger.error(f"Node '{name}' not found in map. Cannot read.")
            raise ValueError(f"Node '{name}' not found in map.")
        return self.client.get_node(node_info["node_id"]).get_value()

    def write(self, name, value):
        node_info = self.node_map.get(name)
        if not node_info:
            # print(f"[ERROR] Node '{name}' not found in map. Cannot write.")
            self.logger.error(f"Node '{name}' not found in map. Cannot write.") 
            return

        node_id_str = node_info["node_id"]
        expected_type_val = node_info["data_type"]
        
        node = self.client.get_node(node_id_str)

        # Get expected VariantType from the node
        expected_type = ua.VariantType(expected_type_val)

        # Auto-cast Python value based on VariantType
        typed_value = self._cast_to_type(value, expected_type)

        # Wrap in Variant with correct type
        variant = ua.Variant(typed_value, expected_type)
        
        # Write to server
        node.set_value(variant)
        if self.console_print:
            print(f"[INFO] Wrote value '{typed_value}' to '{name}' as {expected_type.name}")

    # def batch_write(self, write_requests: list[tuple[str, Any]]):
    #     nodes_to_write = []
    #     variants_to_write = []

    #     for name, value in write_requests:
    #         node_info = self.node_map.get(name)
    #         if not node_info:
    #             print(f"[WARN] Skipping batch write for '{name}': not found in map.")
    #             continue

    #         expected_type = ua.VariantType(node_info["data_type"])
    #         typed_value = self._cast_to_type(value, expected_type)
    #         variant = ua.Variant(typed_value, expected_type)

    #         nodes_to_write.append(self.client.get_node(node_info["node_id"]))
    #         variants_to_write.append(variant)

    #     if nodes_to_write:
    #         self.client.write_value(nodes_to_write, variants_to_write)
    
    def batch_write_2(self, write_requests: list[tuple[str, Any]]):
        for name, value in write_requests:
            self.write(name, value)
            

    def get_node_map(self,name):
        return self.node_map[name]
