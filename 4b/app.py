import sys

#function to read data from txt files
def read_data(file,delimeter):
	f = open(file, "r")
	data = f.read().split(delimeter)
	return data

#function to create adjacency list based on the task dependencies
def create_adjacency_list(tasks,relations):
	adj_list = {}
	for task in tasks:
		adj_list[task]=set()

	for relation in relations:
		ele = relation.split("->")
		adj_list[ele[1]].add(ele[0])
	return adj_list

#function to find already visited nodes
def find_visited(start,adj_list):
	visited = set()
	tmp_stack = start.copy()
	while tmp_stack:
		node = tmp_stack.pop(-1)
		visited.add(node)
		if adj_list[node]:
			tmp_stack.extend(adj_list[node])
	return visited

#function to get the path/order of tasks based on start,goal and dependencies
def dependency_sort(adj_list, start, goal, visited):
	path,stack = [], []

	if adj_list[goal]:
		stack.extend(adj_list[goal])

	while stack:
		curr_node = stack[-1]
		children_list = adj_list[curr_node]

		if len(children_list) == len(children_list.intersection(visited)) or len(children_list)==0:
			visited.add(curr_node)
			stack.remove(curr_node)
			add_children = set(start).intersection(children_list).difference(set(path))
			if add_children:
				path.extend(add_children)
			if curr_node not in path:
				path.append(curr_node)

		else:
			stack.extend(children_list)

	path.append(goal)
	return path

if __name__ == '__main__':
	
	relations_file = sys.argv[1]
	question_file = sys.argv[2]
	task_ids_file = sys.argv[3]
	output_file = sys.argv[4]

	relation_list = read_data(relations_file,"\n")

	start_goal = read_data(question_file,"\n")
	start = start_goal[0].split(":")[-1].strip().split(",")
	goal = start_goal[1].split(":")[-1].strip()

	task_ids = read_data(task_ids_file,",")

	adj_list = create_adjacency_list(task_ids, relation_list)	
	visited = find_visited(start,adj_list)
	path = dependency_sort(adj_list, start, goal, visited)

	f = open(output_file, "w")
	f.write(str(path))
	f.close




