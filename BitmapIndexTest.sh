rm datafiles/*
./nanodb < schemas/stores/make-stores.sql > /dev/null
./nanodb < schemas/stores/stores-28K.sql > /dev/null

echo "---------"
echo "Doing tests without bitmap indexes"
echo "---------"

echo "select * from employees where home_loc_id = 45;"
echo "select * from employees where home_loc_id = 45;" | ./nanodb 2>&1 | grep "sec to evaluate." 
echo "---------"

echo "select * from employees where home_loc_id = 45 and work_loc_id = 166"
echo "select * from employees where home_loc_id = 45 and work_loc_id = 166;" | ./nanodb 2>&1 | grep "sec to evaluate." 
echo "---------"

echo "select * from employees join cities on home_loc_id = city_id and work_loc_id = city_id"
echo "select * from employees join cities on home_loc_id = city_id and work_loc_id = city_id;" | ./nanodb 2>&1 | grep "sec to evaluate."
echo "---------"

echo "Doing test with bitmap indexes"
echo "---------"

echo "create bitmap index on employees(home_loc_id);" | ./nanodb > /dev/null 2>&1
echo "create bitmap index on employees(work_loc_id);" | ./nanodb > /dev/null 2>&1

echo "select * from employees where home_loc_id = 45"
echo "select * from employees where home_loc_id = 45;" | ./nanodb 2>&1 | grep "sec to evaluate."
echo "---------"

echo "select * from employees where home_loc_id = 45 and work_loc_id = 166"
echo "select * from employees where home_loc_id = 45 and work_loc_id = 166;" | ./nanodb 2>&1 | grep "sec to evaluate."
echo "---------"

echo "select * from employees join cities on home_loc_id = city_id and work_loc_id = city_id where city_name = 'New York'"
echo "select * from employees join cities on home_loc_id = city_id and work_loc_id = city_id where city_name = 'New York';" | ./nanodb 2>&1 | grep "sec to evaluate."
echo "---------"


