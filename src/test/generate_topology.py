import csv
import random

def generate_topology(output_file):
    # Randomly generate the number of nodes for each type
    num_hosts = random.randint(100, 200)
    num_leafs = random.randint(50, 80)
    num_spines = random.randint(30, 50)
    num_cores = random.randint(10, 20)

    nodes = {
        "HOST": list(range(num_hosts)),
        "LEAF": list(range(num_hosts, num_hosts + num_leafs)),
        "SPINE": list(range(num_hosts + num_leafs, num_hosts + num_leafs + num_spines)),
        "CORE": list(range(num_hosts + num_leafs + num_spines, num_hosts + num_leafs + num_spines + num_cores)),
    }

    links = []

    # Connect HOST to LEAF
    for host in nodes["HOST"]:
        leaf = random.choice(nodes["LEAF"])
        delay = round(random.uniform(0.01, 0.1), 2)
        bw = round(random.uniform(500, 1000), 1)
        links.append((host, "HOST", leaf, "LEAF", delay, bw))
        links.append((leaf, "LEAF", host, "HOST", delay, bw))

    # Connect LEAF to SPINE
    for leaf in nodes["LEAF"]:
        spine = random.choice(nodes["SPINE"])
        delay = round(random.uniform(0.01, 0.1), 2)
        bw = round(random.uniform(1000, 2000), 1)
        links.append((leaf, "LEAF", spine, "SPINE", delay, bw))
        links.append((spine, "SPINE", leaf, "LEAF", delay, bw))

    # Connect SPINE to CORE
    for spine in nodes["SPINE"]:
        core = random.choice(nodes["CORE"])
        delay = round(random.uniform(0.01, 0.1), 2)
        bw = round(random.uniform(1000, 2000), 1)
        links.append((spine, "SPINE", core, "CORE", delay, bw))
        links.append((core, "CORE", spine, "SPINE", delay, bw))

    # Connect CORE to CORE
    for core1 in nodes["CORE"]:
        for core2 in nodes["CORE"]:
            if core1 != core2:
                delay = round(random.uniform(1, 5), 2)
                bw = round(random.uniform(1000, 2000), 1)
                links.append((core1, "CORE", core2, "CORE", delay, bw))

    # Write to CSV
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["a_node_id", "a_node_type", "z_node_id", "z_node_type", "delay(ms)", "bw(GBps)"])
        writer.writerows(links)

if __name__ == "__main__":
    output_path = "data/topology/link_list_tmp.csv"
    generate_topology(output_path)
