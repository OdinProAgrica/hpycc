import hpycc.delete
import old_tests.make_data as md
import hpycc.run
from hpycc.utils.HPCCconnector import HPCCconnector

if __name__ == "__main__":
    test_script_location = ''
else:
    test_script_location = './old_tests/'

do_syntaxcheck = True
log_to_file = True
debg = True
server = 'localhost'


hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)
logical_file = '~TEMP::HPYCC::TEST::FILE'  # this needs to match the below script


def test_run_script():
    # TODO: should check it's been run on the server to be a proper test.
    script = test_script_location + 'ECLtest_runScript_1output_save.ecl'

    hpycc.run.run_script(script, server, port="8010", repo=None,
                         username="hpycc_get_output", password='" "', silent=False,
                         legacy=False, syntax_check=True, log_to_file=log_to_file, debg=debg)

    assert md.check_exists(logical_file, hpcc_connection)


def test_delete_script_1():
    # TODO: should check it's been run on the server to be a proper test.
    md.upload_data(logical_file, 15, hpcc_connection)

    hpycc.delete.delete_logical_file(logical_file, server, port="8010", repo=None,
                                     username="hpycc_get_output", password='" "',
                                     legacy=False, syntax_check=True,
                                     silent=False, debg=False, log_to_file=False)

    assert not md.check_exists(logical_file, hpcc_connection)


def test_delete_script_no_file():
    file_doesnt_exist = '~A::FILE::THAT::DOESNT::EXIST::3::1::4::1::5::9'

    assert not md.check_exists(file_doesnt_exist, hpcc_connection)  # Fail if file exists before test

    hpycc.delete.delete_logical_file(file_doesnt_exist, server, port="8010", repo=None,
                                     username="hpycc_get_output", password='" "',
                                     legacy=False, syntax_check=True,
                                     silent=False, debg=False, log_to_file=False)

    assert not md.check_exists(file_doesnt_exist, hpcc_connection)  # Fail if exists after.
