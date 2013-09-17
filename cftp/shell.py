#!/usr/bin/env python

import argparse
from cftp import Cftp
from cmd2 import Cmd, make_option, options
import sys
import pyrax
import utils
import exceptions
from prettytable import PrettyTable

class Shell(Cmd):
    """ Initialize and run the psuedo command prompt to process arguments"""

    def __init__(self, username, api_key, region, snet=False, delimiter="/"):
        """ Initalize some basic global variables"""
        Cmd.__init__(self)
        self.cftp = Cftp()
        self.cftp.authenticate(username, api_key, region)
        self.username = username

    def update_prompt(self):
        self.prompt = self.prompt = self.username + "@" + \
            self.current_loc["region"] + ":" + self.parsed_loc + "> "

    def update_loc(self):
        self.current_loc = self.cftp.get_current_loc()
        self.delimiter = self.current_loc["delimiter"]
        self.parsed_loc = self.delimiter
        if self.current_loc["container"]:
            self.parsed_loc += self.current_loc["container"] + \
                                self.current_loc["delimiter"]
        if self.current_loc["prefix"]:
            self.parsed_loc += self.current_loc["prefix"] + \
                                self.current_loc["delimiter"]

    def preloop(self):
        """ Likely Temporary, provide a test parameter to auth with Pyrax """
        self.update_loc()
        self.update_prompt()

    def postloop(self):
        print ""
        print "Disconnected from Cloud Files. Goodbye."

    def postcmd(self, stop, line):
        self.update_loc()
        self.update_prompt()
        return stop

    def do_change_prefix(self, new_prefix=""):
        self.cftp.change_prefix(new_prefix)

    def do_change_region(self, region):
        self.cftp.change_region(region)

    def do_change_container(self, new_container):
        if self.cftp.change_container(new_container):
            print "Changed container to", new_container
        else:
            print "Container", new_container, "does not exist. You must create it before changing to it."

    def do_get(self, args=''):
        """
        Fetch the given file.
        """
        if args == '': args = self.current_dir
        ls_container, ls_prefix = utils.cf_parse_path(self.delimiter,
            self.current_dir, args)

    def do_cd(self, new_location=None):
        """Change the working psudo-directory in Cloud Files."""
        if not new_location:
            self.cftp.clear_container()
            self.update_loc()
        else:
            new_container, new_prefix = utils.cf_parse_path(self.delimiter,
                self.parsed_loc, new_location)
            if self.cftp.change_container(new_container):
                self.cftp.change_prefix(new_prefix)
                self.update_loc()
            else:
                print "Container", new_container, "does not exist. You must create it before changing to it."


    @options([make_option('-l',
            '--long',
            action="store_true",
            help="print long listing"),
        make_option('-m',
            '--human',
            action="store_true",
            help="with -l, print sizes in human readable format"),
        make_option('-H',
            '--header',
            action="store_true",
            help="with -l, print the header of the table"),
        ])
    def do_list(self, args='', opts=None):
        """
        Perform a listing of the current location or a given location
        """
        if args == '': args = self.current_dir
        ls_container, ls_prefix = utils.cf_parse_path(self.delimiter,
            self.current_dir, args)

        if not ls_container:
            utils.cf_listing(self.cf.list_containers_info(),
                    self.delimiter,
                    long_listing=opts.long,
                    human=opts.human,
                    header=opts.header)
        elif ls_container in self.cf.list_containers():
            if len(ls_prefix) > 0:
                ls_prefix = self.delimiter.join(ls_prefix)
                if args[-1] == self.delimiter:
                    ls_prefix += self.delimiter
                    ls_obj = None
                else:
                    try:
                        ls_obj = self.cf.get_object(ls_container, ls_prefix)
                    except:
                        # Need to put on the trailing slash
                        ls_prefix += self.delimiter
                        ls_obj = None
                    else:
                        # Ignore returned subdir objects
                        if getattr(ls_obj, 'content_type') == 'pseudo/subdir':
                            ls_prefix += self.delimiter
                            ls_obj = None
            else:
                ls_prefix = None
                ls_obj = None
            if ls_obj:
                objs = [ls_obj]
            else:
                cont_obj = self.cf.get_container(ls_container)
                objs = cont_obj.list_subdirs(
                    delimiter=self.delimiter,
                    prefix=ls_prefix) + \
                    cont_obj.get_objects(
                    delimiter=self.delimiter,
                    prefix=ls_prefix)
            utils.cf_listing(objs,
                    self.delimiter,
                    long_listing=opts.long,
                    human=opts.human,
                    header=opts.header)
        else:
            print "Container", ls_container, "does not exist."



    def do_lls(self, args):
        """
        Runs local ls.
        """
        Cmd.do_shell(self, 'ls -1 ' + args)

    def do_lpwd(self, args):
        """
        Runs local pwd.
        """
        Cmd.do_shell(self, 'pwd ' + args)

    def do_lcd(self, args):
        """
        Runs local cd.
        """
        Cmd.do_shell(self, 'cd ' + args)

    """ Here there be aliases """
    do_ls = do_list
    do_dir = do_list
    # do_cd = do_change_prefix
    do_cc = do_change_container
    do_cr = do_change_region


def main():
    parser = argparse.ArgumentParser(description='Connects to Cloud Files.')
    parser.add_argument('--username',
        '-u',
        dest='username',
        action='store',
        type=str,
        required=True,
        help='Rackspace Cloud username.')
    parser.add_argument('--api-key',
        '-k',
        dest='api_key',
        action='store',
        type=str,
        required=True,
        help='Rackspace Cloud API key.')
    parser.add_argument('--region',
        '-r',
        dest='region',
        action='store',
        type=str,
        required=True,
        help='Cloud Files region.')
    args = parser.parse_args()
    sys.argv = [sys.argv[0]] # extra args upset cmd2 for some reason.
    
    cftp = Shell(args.username, args.api_key, args.region.upper())
    # cftp = Shell()
    cftp.cmdloop()

if __name__ == '__main__':
    main()