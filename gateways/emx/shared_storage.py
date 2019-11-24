class shared_storage():
    def __init__(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}
        self.eid_to_uid_amend = {}
        self.eids_to_amend = {}
        self.orders = {}
        self.fills = {}

    def reset(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}
        self.eid_to_uid_amend = {}
        self.eids_to_amend = {}
        self.orders = {}
        self.fills = {}
