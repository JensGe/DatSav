from enum import Enum


class TLD(str, Enum):
    Germany = "de"
    Commercial = "com"
    France = "fr"
    Organisation = "org"
    Sweden = "se"


class STF(str, Enum):
    random = "random"
    old_pages_first = "old_pages_first"
    change_rate = "change_rate"


class LTF(str, Enum):
    random = "random"
    top_level_domain = "top_level_domain"
    large_sites_first = "large_sites_first"
    old_sites_first = "old_sites_first"
    geo_distance = "geo_distance"
    average_change_rate = "average_change_rate"
    consistent_hashing = "consistent_hashing"
