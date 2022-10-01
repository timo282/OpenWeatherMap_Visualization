# functions for duplicate check

def check_if_duplicate(new: dict, old: dict) -> bool:
    """
    Checks if two dictionaries (new and old) are duplicates according to the criteria that are important for this use case
    (duplicate: every value has to be the same except from "fetched_at", which is the time when the forecast has been fetched).
    """
    if new.keys()!=old.keys(): # if the keys are not the same, it is already safe to say that the dictionaries are not duplicates --> return False
        return False 
    compare = new.keys() - {'fetched_at'}  # keys of which the corresponding values are compared (all keys except for "fetched_at")
    for key in compare:
        if new[key] != old[key]:  # if (for at least one key) the values are not the same --> no duplicates, return False
            return False
    return True  # if loop has not already broken: all values are the same --> duplicates, return True


def check_if_duplicate_in_list(new: dict, old: list) -> bool:
    """
    Checks if a dictionary (:param new) has already a duplicate dictionary in a list of dictionaries (:param old).
    Uses the check_if_duplicate()-function above.
    """
    for old_dict in old:
        if check_if_duplicate(new, old_dict):
            return True  # if a dictionary in the list is a duplicate --> return True
    return False # if loop has not already broken: no duplicate dictionary is in the list --> return False