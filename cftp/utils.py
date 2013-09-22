#!/usr/bin/env python
import pyrax
from prettytable import PrettyTable

"""
A set of utility fuctions to help with cftp
"""

def human_read(num):
    for x in ["B","KB","MB","GB"]:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, "TB")

def cf_listing(ls_list, delimiter, long_listing=False, human=False, header=False):
    if len(ls_list) > 0:
        human_fields=["bytes", "total_bytes"]
        temp_list = [] # create our temp list of lists
        for obj in ls_list:
            if type(obj) == dict:
                if long_listing:
                    ls_vars_list = ["count", "bytes", "name"]
                else:
                    ls_vars_list = ["name"]
                attr_list = []
                for attr in ls_vars_list:
                    if human and obj.get(attr) and attr in human_fields:
                        attr_list.append(human_read(obj.get(attr)))
                    else:
                        attr_list.append(obj.get(attr))
                temp_list.append(attr_list)
            else: # Assuming it's a StorageObject
                if long_listing:
                    ls_vars_list = ["etag", "content_type", "total_bytes",
                        "last_modified", "name"]
                else:
                    ls_vars_list = ["name"]
                attr_list = []
                for attr in ls_vars_list:
                    if human and getattr(obj, attr) and attr in human_fields:
                        attr_list.append(human_read(getattr(obj, attr)))
                    elif attr == "name":
                        if getattr(obj, "content_type") == "pseudo/subdir":
                            attr_list.append(getattr(obj,
                                attr).split(delimiter)[-1] + delimiter)
                        else:
                            attr_list.append(getattr(obj,
                                attr).split(delimiter)[-1])
                    else:
                        attr_list.append(getattr(obj, attr))
                temp_list.append(attr_list)
        if long_listing:
            out_table = PrettyTable(ls_vars_list)
            out_table.border = False
            out_table.header = True if header else False
            out_table.left_padding_width = 0
            out_table.right_padding_width = 2
            out_table.align = "l"
            for row in temp_list:
                out_table.add_row(row)
            return out_table
        else:
            return "  ".join([s[0] for s in temp_list])
    else:
        return ""

def container_ls(objs, delimiter, long_listing=False, human=False,
    header=False):
    if len(objs) > 0:
        var_list = ["count", "bytes", "name"] if long_listing else ["name"]
        temp_list = [] # create our temp list of lists
        for obj in objs:
            attr_list = []
            for attr in var_list:
                if human and obj.get(attr) and attr == "bytes":
                    attr_list.append(human_read(obj.get(attr)))
                else:
                    attr_list.append(obj.get(attr))
            temp_list.append(attr_list)
        if long_listing:
            out_table = PrettyTable(var_list)
            out_table.border = False
            out_table.header = True if header else False
            out_table.left_padding_width = 0
            out_table.right_padding_width = 2
            out_table.align = "l"
            for row in temp_list:
                out_table.add_row(row)
            return out_table
        else:
            return "  ".join([s[0] for s in temp_list])
    else:
        return ""

def object_ls(objs, delimiter, long_listing=False, human=False, header=False):
    if len(objs) > 0:
        var_list = ["etag", "content_type", "total_bytes", "last_modified",
            "name"] if long_listing else ["name"]
        temp_list = []
        for obj in objs:
            attr_list = []
            for attr in var_list:
                if human and getattr(obj, attr) and attr == "total_bytes":
                    attr_list.append(human_read(getattr(obj, attr)))
                elif attr == "name":
                    if getattr(obj, "content_type") == "pseudo/subdir":
                        attr_list.append(getattr(obj,
                            attr).split(delimiter)[-1] + delimiter)
                    else:
                        attr_list.append(getattr(obj, 
                            attr).split(delimiter)[-1])
                else:
                    attr_list.append(getattr(obj, attr))
            temp_list.append(attr_list)
        if long_listing:
            out_table = PrettyTable(var_list)
            out_table.border = False
            out_table.header = True if header else False
            out_table.left_padding_width = 0
            out_table.right_padding_width = 2
            out_table.align = "l"
            for row in temp_list:
                out_table.add_row(row)
            return out_table
        else:
            return "  ".join([s[0] for s in temp_list])
    else:
        return ""

def ls_table(var_list, table_list, header=False):
    out_table = PrettyTable(var_list)
    out_table.border = False
    out_table.header = True if header else False
    out_table.left_padding_width = 0
    out_table.right_padding_width = 2
    out_table.align = "l"
    for row in table_list:
        out_table.add_row(row)
    return out_table

def cf_normpath(delimiter, path):
    """
    Normalize path.
    Much of this code is borrowed from os.posixpath, adapted for use with CF.
    """
    dot = "."
    if path == "":
        return dot
    initial_delimiter = path.startswith(delimiter)
    comps = path.split(delimiter)
    new_comps = []
    for comp in comps:
        if comp in ("", "."):
            continue
        if (comp != ".." or (not initial_delimiter and not new_comps) or
             (new_comps and new_comps[-1] == "..")):
            new_comps.append(comp)
        elif new_comps:
            new_comps.pop()
    comps = new_comps
    path = delimiter.join(comps)
    if initial_delimiter:
        path = delimiter + path
    return path or dot

def cf_join(delimiter, a, *p):
    """
    Join two or more pathname components, inserting delimiter as needed.
    If any component is an absolute path, all previous path components
    will be discarded.  An empty last part will result in a path that
    ends with a separator.
    Much of this code is borrowed from os.posixpath, adapted for use with CF.
    """
    path = a
    for b in p:
        if b.startswith(delimiter):
            path = b
        elif path == "" or path.endswith(delimiter):
            path +=  b
        else:
            path += delimiter + b
    return path


def cf_split(delimiter, p):
    """Split a pathname.  Returns tuple "(head, tail)" where "tail" is
    everything after the final delimiter.  Either part may be empty.
    Much of this code is borrowed from os.posixpath, adapted for use with CF.
    """
    i = p.rfind(delimiter) + 1
    head, tail = p[:i], p[i:]
    if head and head != delimiter*len(head):
        head = head.rstrip(delimiter)
    return head, tail

def cf_parse_path(delimiter, path_a, path_b):
    """Takes two paths and a delimiter, joins them, then returns a tuple
    containing a string of the resulting container and prefix. Will return None
    for either if they're empty.
    This assumes that path_b is the incomplete path.
    """
    end_delimiter = path_b.endswith(delimiter)
    joined_path = cf_join(delimiter, path_a, path_b)
    new_path = cf_normpath(delimiter, joined_path)
    comps = new_path.split(delimiter)
    new_comps = []
    # strip out empty fields
    for comp in comps:
        if comp == "" or comp == ".":
            continue
        new_comps.append(comp)
    try:
        container = new_comps[0]
    except IndexError:
        container = None
    try:
        prefix = delimiter.join(new_comps[1:])
    except IndexError:
        prefix = None
    if prefix and end_delimiter:
        prefix += delimiter
    # first item is container, second item is list of container.
    return container, prefix