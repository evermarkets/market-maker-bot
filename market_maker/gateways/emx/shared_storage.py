class SharedStorage:
    # uid is an user id
    # eid is an exchange id

    def __init__(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}
        self.eids_to_amend = {}

    def reset(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}
        self.eids_to_amend = {}
