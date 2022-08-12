import json


class PropertyIndex:
    def __init__(self, persisted_file=None) -> None:
        if persisted_file is None:
            self.property_ids = {}
            self.property_names = {}
            self.counter = 0
        else:
            self.load(persisted_file)

    def get_property_id(self, column_name):
        if property not in self.property_ids:
            self.property_ids[property] = self.counter
            self.property_names[self.counter] = property
            self.counter += 1
        return self.property_ids[property]

    def get_property_name(self, id):
        return self.property_names.get(id)

    def num_labels(self):
        return self.counter + 1

    def load(self, persisted_file):
        with open(persisted_file, 'r') as f:
            self.property_ids = json.loads(f.readline())
            names = json.loads(f.readline())
            self.property_names = {}
            for key, value in names.items():
                self.property_names[int(key)] = value

            self.counter = int(json.loads(f.readline()))

    def persist(self, filename):
        id_mapping = json.dumps(self.property_ids)
        name_mapping = json.dumps(self.property_names)
        with open(filename, "w") as f:
            f.write(id_mapping + "\n")
            f.write(name_mapping + "\n")
            f.write(str(self.counter) + "\n")
