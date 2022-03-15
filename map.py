class Map(dict):
    ''' Cleans up dictionary operations and allows of dot notation '''

    def __setattr__(self, key, value):
        super().__setitem__(key, value)

    def __getattr__(self, key):
        if key == '__deepcopy__':
            return None
        else:
            return super().__getitem__(key)

    def __getstate__(self):
        return dict(self)

    def __setstate__(self, state):
        self.clear()
        self.update(state)

    def copy(self):
        return Map(super().copy())

    def deepcopy(self):
        return Map(dict(self).deepcopy())