import time
import pandas as pd
import logging
import random


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def randomly_select_set(sets):
    """
    Randomly selects a set from the given list of sets.

    Args:
        sets (list): A list of sets.

    Returns:
        tuple: A tuple containing the randomly selected set and its index.
    """
    selected_index = random.randint(0, len(sets) - 1)
    return sets[selected_index], selected_index

class Inclusivenodeset:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = self.load_data()  # Load data once during initialization

    def load_data(self):
        """Load the CSV data into a DataFrame."""
        try:
            df = pd.read_csv(self.file_path, header=1)
            df["Parent Node ID"] = df[" Parent Node ID"].where(pd.notnull(df[" Parent Node ID"]), None)
            return df
        except KeyError:
            df = pd.read_csv(self.file_path)
            df["Parent Node ID"] = df["parentNodeId"].where(pd.notnull(df["parentNodeId"]), None)
            return df
        except FileNotFoundError as e:
            logging.error(f"File not found: {self.file_path}")
            raise e

    def update_checkpoint_table(self, df, sleep_start_times):
        """Update the checkpoint table DataFrame with new timings."""
        logging.info("Updating checkpoint table with new timings...")
        for node, timing in sleep_start_times.items():
            try:
                if node in df["Node Name"].values:
                    df.loc[df["Node Name"] == node, "timeToSleep"] = timing["timeToSleep"]
                    df.loc[df["Node Name"] == node, "timeToWake"] = timing["timeToWake"]
                    df.loc[df["Node Name"] == node, "isSleeping"] = timing["isSleeping"]
            except KeyError:
                if node in df["nodeName"].values:
                    df.loc[df["nodeName"] == node, "timeToSleep"] = timing["timeToSleep"]
                    df.loc[df["nodeName"] == node, "timeToWake"] = timing["timeToWake"]
                    df.loc[df["nodeName"] == node, "isSleeping"] = timing["isSleeping"]

        # Save the updated DataFrame back to the CSV file
        logging.info(f"Saving updated data back to {self.file_path}")
        df.to_csv(self.file_path, index=False)
        return df

    def dataExtraction(self):
        """Convert the DataFrame to a dictionary."""
        return self.df.set_index("nodeName")["Parent Node ID"].to_dict()

    def find_chain(self, node):
        """Find the transmission chain for a given node."""
        nodes_dict = self.dataExtraction()
        chain = []
        while node:
            if node not in {"CloudDBServer", "L1Node"}:
                if  not node.startswith('L2Node_'):
                    chain.append(node)
            node = nodes_dict.get(node)  # Safely fetch the parent node
            #print("node: ", node)
        #print("chain: ", chain)
        return chain[::-1]  # Reverse the chain to start from the root

    def find_mutually_inclusive(self, sets):
        """Find mutually inclusive groups."""
        inclusive_groups = []
        for s in sets:
            merged = False
            for group in inclusive_groups:
                if not set(s).isdisjoint(group):
                    group.update(s)
                    merged = True
                    break
            if not merged:
                inclusive_groups.append(set(s))

        # Further merge groups that became inclusive
        merged_groups = []
        while inclusive_groups:
            current = inclusive_groups.pop(0)
            to_merge = [g for g in inclusive_groups if not current.isdisjoint(g)]
            for group in to_merge:
                current.update(group)
                inclusive_groups.remove(group)
            merged_groups.append(sorted(current))
        return merged_groups

    def generateList(self):
        """Generate transmission chains for Layer 4 nodes."""
        nodes_dict = self.dataExtraction()
        layer_4_nodes = [node for node in nodes_dict if node.startswith("L4Node")]
        chains = [self.find_chain(node) for node in layer_4_nodes]
        # An attemp to add layer2 parents while having extended number of chains
        '''print("chains: ", chains)
        for nodes in chains:
            if nodes[0].startswith("L3Node_"):
                nodes.append(nodes_dict[f"{nodes[0]}"])
        print("chains: ", chains)   '''
        return self.find_mutually_inclusive([set(chain) for chain in chains])

    def generateSet(self):
        """Generate a list of sets for mutually inclusive groups."""
        nodes_dict = self.dataExtraction()
        inclusive_list = self.generateList()
        # An attemp to add layer2 parents while having extended number of chains
        #print("\ninclusive_list: ", inclusive_list)
        for nodes in inclusive_list:
            for node in nodes:
                if node.startswith("L3Node_"):
                    #print(nodes_dict[f"{node}"])
                    nodes.append(nodes_dict[f"{node}"])
        #print("inclusive_list: ", inclusive_list) 
        logging.info(f"Number of inclusive groups: {len(inclusive_list)}")
        return [set(item) for item in inclusive_list]

    def simulate_state(self, group, state, duration):
        """Simulate the state of a group."""
        logging.info(f"Group {group} is in {state} state for {duration} seconds.")
        time.sleep(duration)

    def sleep_wake_simulation(self, node_groups, T_wake, T_sleep):
        """Simulate sleep-wake cycles for all node groups."""
        #logging.info(node_groups)
            
        while True:
            for group_id, group in enumerate(node_groups):
                #print("\ngroup_id: ", group_id)
                #print("group: ", group)
                #self.simulate_state(group, state="wake", duration=T_wake)
                #self.simulate_state(group, state="sleep", duration=T_sleep)
                randomly_selected_set, selected_index = randomly_select_set(node_groups)
                print(f"Randomly Selected Set (Index: {selected_index}):")
                print(randomly_selected_set)
                self.simulate_state(randomly_selected_set, state="wake", duration=T_wake)
                self.simulate_state(randomly_selected_set, state="sleep", duration=T_sleep)
    
    """def update_checkpoint_table(self, df, sleep_start_times):
        # Update the checkpoint table DataFrame with new timings
        logging.info(df)
        for node, timing in sleep_start_times.items():
            try:
                if node in df["Node Name"].values:
                    df.loc[df["Node Name"] == node, "timeToSleep"] = timing["timeToSleep"]
                    df.loc[df["Node Name"] == node, "timeToWake"] = timing["timeToWake"]
                    df.loc[df["Node Name"] == node, "isSleeping"] = timing["isSleeping"]
            except:
                if node in df["nodeName"].values:
                    df.loc[df["nodeName"] == node, "timeToSleep"] = timing["timeToSleep"]
                    df.loc[df["nodeName"] == node, "timeToWake"] = timing["timeToWake"]
                    df.loc[df["nodeName"] == node, "isSleeping"] = timing["isSleeping"]
        #logging.info("df: ", df)
        return df"""

    def assign_sleep_start_times(self, inclusive_groups, T_wake, T_sleep):
        # Initialize the start time
        sleep_start_times = {}
        current_time = time.time() + T_wake  # Start after T_wake seconds
        for idx, group in enumerate(inclusive_groups):
            sleep_start_time = current_time + (idx * T_sleep)
            for node in group:
                sleep_start_times[node] = {
                    "timeToSleep": sleep_start_time,
                    "timeToWake": sleep_start_time + T_sleep,
                    "isSleeping": False,
                }
        return sleep_start_times

    
# Other methods remain unchanged...

# Main execution code
file_path = '/home/mosud/IdeaProjects/mio/treebased2/checkpoint_table.csv'
T_wake = 70  # Active period in seconds
T_sleep = 90  # Sleep period in seconds

inclusivebuild = Inclusivenodeset(file_path)
node_groups = inclusivebuild.generateSet()

# Load data and generate mutually inclusive groups
df = inclusivebuild.load_data()
inclusive_groups = inclusivebuild.generateSet()

# Assign sleep start times to nodes
sleep_start_times = inclusivebuild.assign_sleep_start_times(inclusive_groups, T_wake, T_sleep)

# Update the checkpoint table with calculated timings and save to CSV
updated_df = inclusivebuild.update_checkpoint_table(df, sleep_start_times)

# Simulate sleep-wake cycles
inclusivebuild.sleep_wake_simulation(node_groups, T_wake, T_sleep)
