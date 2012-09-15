
rm -rf dot png
rm test.btree test.bptree
find . -name "*.dot" | xargs rm

for dir in `find . -type d -regextype egrep -regex "^./(b|t).*"`
do
  echo ${dir:2}
  go test file-structures/${dir:2}
done

mkdir dot
for file in `find . -name "*.dot"`
do
  mv $file dot/
done

for file in dot/*.dot
do
  echo $file | cut -d "/" -f 2 - | xargs -I"%s" dot -Tpng $file -o png/%s.png
done

