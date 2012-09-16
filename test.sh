
rm -rf dot png 2> /dev/null
rm test.btree test.bptree 2> /dev/null
find . -name "*.dot" | xargs rm 2> /dev/null

for dir in `find . -type d -regextype egrep -regex "^./[a-zA-Z].*"`
do
  echo ${dir:2}
  go install file-structures/${dir:2}
  go test file-structures/${dir:2}
done

mkdir dot
for file in `find . -name "*.dot"`
do
  mv $file dot/
done

if [[ $1 == "pics" ]] ; then
  echo "pics"
  mkdir png
  for file in dot/*.dot
  do
    echo $file | cut -d "/" -f 2 - | xargs -I"%s" dot -Tpng $file -o png/%s.png
  done
fi

