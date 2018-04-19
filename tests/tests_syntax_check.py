import hpycc.getHPCC as getHPCC

print(getHPCC.get_script_result('test.ecl', '10.53.57.31:8010', 'C:/z/odin/HPCC'))
print(getHPCC.get_file('proagrica::entities::2::productcategory', 'http://10.53.57.31:8010', CSVlogicalFile = False))

print(getHPCC.get_script_result('test.ecl', '10.53.57.31:8010', 'C:/z/odin/HPCC', saveIt = True))