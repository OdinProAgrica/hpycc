a := DATASET([{1,2},
              {3,4},
              {5,6},
              {7,8},
              {9,10},
              {11,12}
              ], {INTEGER a; INTEGER b});

OUTPUT(a, ,'~a::test::file', EXPIRE(1), OVERWRITE);

OUTPUT(a, , '~a::test::filecsv', OVERWRITE,CSV(HEADING(SINGLE), QUOTE('"')), EXPIRE(1));

