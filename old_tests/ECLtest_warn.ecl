a := DATASET([{1,2}], {INTEGER a; INTEGER b});
a;
b := DATASET([{1,2}], {INTEGER a; INTEGER b});
b;

groupREC := RECORD
  STRING a := a.a;
  STRING b := a.b;
  INTEGER n := count(group);
 END;

TABLE(a, groupREC, a)