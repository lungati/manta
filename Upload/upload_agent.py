#!/usr/bin/env python

import calendar
import datetime
import logging
from optparse import OptionParser
import os
import platform
import shutil
import subprocess
import sys

import find_delta
import iso8601
import upload_threaded

config = None

INPROGRESS = None
ARCHIVE = None

class SqlCommand():
    def __init__(self, command, name):
        self.command = " ".join(command.splitlines())
        self.name = name
        self.command_template = config['sql']['command_template']
        self.exec_template = config['sql']['exec_template']
        self.post_command = None
        if 'post_command' in config['sql']:
            self.post_command = config['sql']['post_command']
        self.match_lines = None
        if 'match_lines' in config['sql']:
            self.match_lines = config['sql']['match_lines']
        
    def Execute(self, output_path):
        statement_file = os.path.join(output_path, self.name + ".sql")
        return_file = os.path.join(output_path, self.name + ".csv")
        sql_output_file = return_file + ".sqltmp"
        post_output_file = return_file + ".posttmp"
        match_output_file = return_file + ".matchtmp"
        temp_files = [sql_output_file, post_output_file, match_output_file]
        stmt = self.command_template % (self.command)
        f = file(statement_file, 'w')
        f.write(stmt)
        f.close()
        assert path_exists(statement_file)
        cmd = self.exec_template % (statement_file, sql_output_file)
        logging.info("Executing %s: %s" % (self.name, cmd))
        result = subprocess.call(cmd, shell=True)
        assert result == 0, "Command error, result was: %d" % (result)
        if self.post_command:
            cmd = self.post_command % (sql_output_file, post_output_file)
            result = subprocess.call(cmd, shell=True)
            logging.info("Executed post command %s: %s" % (self.name, cmd))
            assert result == 0, "Post command error, result was: %d" % (result)
        else:
            shutil.copyfile(sql_output_file, post_output_file)
        if self.match_lines:
            f = open(post_output_file, 'r')
            output = open(match_output_file, 'w')
            header = f.readline().strip()
            print >> output, header
            for line in f:
                if self.match_lines in line:
                    output.write(line)
            f.close()
            output.close()
            logging.info("Executed match lines %s: %s" % (self.name, self.match_lines))
            assert path_exists(match_output_file), "Match command error"
        else:
            shutil.copyfile(post_output_file, match_output_file)
        shutil.copyfile(match_output_file, return_file)
        for f in temp_files:
            os.remove(f)
        return return_file

def path_exists(path):
    try:
        result = os.stat(path)
        return True
    except OSError:
        return False

class InvalidStateError(Exception):
  """Invalid state for directory.
  """

class DataDirectory():
    def __init__(self, root, timestamp):
        self.root = root
        self.timestamp = timestamp
        self.dirname = DataDirectory.format_timestamp(self.timestamp)
        self.path = os.path.join(self.root, self.dirname)

        if path_exists(self.path):
            assert os.path.isdir(self.path)
        else:
            os.mkdir(self.path)

    @staticmethod
    def FromPath(path):
        assert os.path.isdir(path)
        (root, dirname) = os.path.split(path)
        timestamp = DataDirectory.parse_timestring(dirname)
        directory = DataDirectory(root, timestamp)
        assert directory.path == path
        return directory

    @staticmethod
    def format_timestamp(timestamp):
        return datetime.datetime.utcfromtimestamp(long(timestamp)).isoformat('T').replace(':','.')

    @staticmethod
    def parse_timestring(timestring):
        return calendar.timegm(iso8601.parse_date(timestring.replace('.',':')).timetuple())

    @staticmethod
    def get_csv_files(path):
        output = []
        files = os.listdir(path)
        for f in files:
            if not f.endswith('.csv'):
                continue
            (kind, suffix) = f.rsplit('.', 1)
            file_path = os.path.join(path, f)
            output += [(file_path, kind)]
        return output

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    # States a directory can be in.
    BUILDING = 0
    UPLOADING = 1
    COMPLETE = 2

    _filename_map = {
        BUILDING  : 'building.txt',
        UPLOADING : 'uploading.txt',
        COMPLETE  : 'complete.txt',
        }

    def state(self):
        states = []
        for key in self._filename_map:
            if path_exists(os.path.join(self.path, self._filename_map[key])):
                states += [key]
        if len(states) == 0:
            return None
        if len(states) > 1:
            raise InvalidStateError('Multiple states found: ' + str(states))
        return states[0]    
        
    def timestamp(self):
        return self.timestamp

    def path(self):
        return self.path

    def delta_directory(self):
        result = []
        dirs = os.listdir(self.path)
        for d in dirs:
            dir_path = os.path.join(self.path, d)
            if os.path.isdir(dir_path) and d.startswith('Delta-'):
                result += [dir_path]
        assert len(result) <= 1
        if len(result) == 1:
            return result[0]
        else:
            return None

    def CreateDeltaDirectory(self, timestamp):
        assert self.delta_directory() is None
        delta_dirname = 'Delta-' + DataDirectory.format_timestamp(timestamp)
        delta_path = os.path.join(self.path, delta_dirname)
        os.mkdir(delta_path)
        assert self.delta_directory() == delta_path

    def SetState(self, state):
        assert self._filename_map.has_key(state)
        current = self.state()
        new_path = os.path.join(self.path, self._filename_map[state])
        if current is None:
            f = open(new_path, 'w')
            f.close()
        else:
            old_path = os.path.join(self.path, self._filename_map[current])
            if old_path != new_path:
                os.rename(old_path, new_path)

    def Delete(self):
        shutil.rmtree(self.path)

    def Move(self, new_root, clobber=False):
        new_path = os.path.join(new_root, self.dirname)
        if clobber and path_exists(new_path):
            shutil.rmtree(new_path)
        old_path = self.path
        os.rename(old_path, new_path)
        assert not path_exists(old_path), old_path
        assert path_exists(new_path), new_path
        self.root = new_root
	self.path = new_path

# Returns directories in sorted older, oldest->newest
def get_directories(path):
    result = []
    dirs = os.listdir(path)
    for d in dirs:
        dir_path = os.path.join(path, d)
        if os.path.isdir(dir_path):
            result += [DataDirectory.FromPath(dir_path)]
    result.sort()
    return result

def get_sql_commands():
    output = []
    for c in config['sql']['commands']:
        output += [SqlCommand(c['command'], c['name'])]
    return output

def Groom():
    upload_queue = []
    dirs = get_directories(INPROGRESS)
    for d in dirs:
        state = d.state()
        if state is None or state is DataDirectory.BUILDING:
            d.Delete()
        elif state is DataDirectory.UPLOADING:
            upload_queue += [d]
        elif state is DataDirectory.COMPLETE:
            d.Move(ARCHIVE, clobber=True)
        else:
            raise InvalidStateError("Unhandled state: " + state)
    return upload_queue

def Build(directory):
    assert directory.state() is None
    directory.SetState(DataDirectory.BUILDING)
    history = get_directories(ARCHIVE)
    last = None
    if len(history) > 0:
        last = history.pop()
    commands = get_sql_commands()
    for c in commands:
        output_file = c.Execute(directory.path)
        assert path_exists(output_file)
    current_files = DataDirectory.get_csv_files(directory.path)
    assert len(current_files) > 0
    last_files = []
    if last:
        last_files = DataDirectory.get_csv_files(last.path)
        assert len(last_files) > 0
        directory.CreateDeltaDirectory(last.timestamp)
    else:
        directory.CreateDeltaDirectory(0)
    delta_dir = directory.delta_directory()
    assert delta_dir is not None
    kind_map = {}
    for f in last_files:
        (path, kind) = f
        kind_map[kind] = [path, None]
    for f in current_files:
        (path, kind) = f
        if kind_map.has_key(kind):
            kind_map[kind][1] = path
        else:
            kind_map[kind] = [None, path]
    for kind in kind_map:
        (old_path, new_path) = kind_map[kind]
        output_file_path = os.path.join(delta_dir, os.path.basename(new_path))
        if new_path is None:
            logging.warning(("No new dump found for %s, assuming " + 
                             "command was dropped.") % (kind))
        elif old_path is None:
            logging.warning(("No old dump found for %s, assuming " + 
                             "command was newly added.") % (kind))
            shutil.copyfile(new_path, output_file_path)
        else:
            # Generate the delta, as we have both files.
            output_file = file(output_file_path, "w")
            find_delta.output_delta_file(old_path, new_path, output_file, config)
            output_file.close()

def Process(directory):
    assert (directory.state() is DataDirectory.BUILDING or
            directory.state() is DataDirectory.UPLOADING)
    if directory.state() is DataDirectory.UPLOADING:
        logging.warning(("Resuming previously interrupted upload " + 
                         "of directory %s") % (directory.dirname))
    directory.SetState(DataDirectory.UPLOADING)
    delta_dir = directory.delta_directory()
    assert delta_dir is not None
    deltas = DataDirectory.get_csv_files(delta_dir)
    assert len(deltas) > 0
    for delta in deltas:
        (delta_path, kind) = delta
        if config['options'].kinds == ['*'] or kind in config['options'].kinds:
            if not Upload(delta_path, kind):
                logging.error(("Was not successful uploading delta %s:%s. " + 
                               "Abandoning.") % 
                              (directory.dirname, os.path.basename(delta_path)))
                return
    # All deltas were uploaded. Mark as complete, and move to archive.
    directory.SetState(DataDirectory.COMPLETE)

def Upload(path, kind):
    url = config['dest_url'] % (kind)
    logging.info("Starting upload of %s" % (url))
    return upload_threaded.upload_file(url, path)

def GarbageCollect():
    dirs = get_directories(ARCHIVE)
    for d in dirs[:-config['history_size']]:
        d.Delete()

def utc_now():
    return calendar.timegm(datetime.datetime.now().timetuple())

def HandleCommand(args):
    (command, args) = args[0], args[1:]
    if command == "sql":
        (kind, output_dir) = args[0], args[1]
        found = False
        for c in get_sql_commands():
            if kind == '*' or c.name == kind:
                c.Execute(output_dir)
                found = True
        if not found:
            logging.fatal("SQL command name %s was not found.", kind)
    else:
        logging.fatal("Command unknown: %s", command)
        

def main():
    args = SetupLoggingAndFlags()
    EnsureDirectoriesExist()

    if len(args) > 0:
        HandleCommand(args)
        return

    start = utc_now()
    logging.info("Starting upload agent run at %s",
                 DataDirectory.format_timestamp(start))

    upload_queue = Groom()
    for d in upload_queue:
        Process(d)
    
    upload_queue = Groom()
    assert len(upload_queue) == 0

    current_run = DataDirectory(INPROGRESS, start)
    Build(current_run)
    Process(current_run)

    upload_queue = Groom()
    assert len(upload_queue) == 0

    GarbageCollect()

    logging.info("Completed upload agent run. Time elapsed: %s",
                 datetime.timedelta(seconds=(long(utc_now() - start))))

def SetupLoggingAndFlags():
    parser = OptionParser()
    parser.add_option("-l", "--loglevel", dest="loglevel",
                      help="loglevel to display", default="INFO")
    parser.add_option("-f", "--logfile", dest="logfile", metavar="FILE",
                      help="file for logging output", default=None)
    parser.add_option("-c", "--config", dest="config",
                      help="config module to use", default="config")
    parser.add_option("-k", "--kinds", dest="kinds",
                      help="list of kinds to actually upload", default="*")
    parser.add_option("--discard_duplicate_existing_rows", dest="discard_duplicate_existing_rows",
                      help="discard duplicate rows from existing input files, taking most recent", 
                      default=False, action='store_true')
    
    (options, args) = parser.parse_args()

    # assuming loglevel is bound to the string value obtained from the
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, options.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level, filename=options.logfile)

    __import__(options.config)
    globals()['config'] = sys.modules[options.config].config

    globals()['INPROGRESS'] = os.path.join(config['root_dir'], 'inprogress')
    globals()['ARCHIVE'] = os.path.join(config['root_dir'], 'archive')

    options.kinds = options.kinds.split(',')

    config['options'] = options

    return args
SetupLoggingAndFlags()

def EnsureDirectoriesExist():
    if not path_exists(ARCHIVE):
        os.makedirs(ARCHIVE)
    if not path_exists(INPROGRESS):
        os.makedirs(INPROGRESS)

def test():
    dirs = get_directories(INPROGRESS)
    for d in dirs:
        d.Move(ARCHIVE)
    return

    dirs = get_directories(ARCHIVE)
    print dirs
    DataDirectory(INPROGRESS, utc_now())
    dirs = get_directories(INPROGRESS)
    print dirs
    commands = get_sql_commands()
    for d in dirs:
        print d.state()
        d.SetState(DataDirectory.BUILDING)
        print d.state()
        for c in commands:
            c.Execute(d.path)
        d.Move(ARCHIVE)
        d.SetState(DataDirectory.COMPLETE)
        print d.state()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit()
