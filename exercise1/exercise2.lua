---------------------------------------------
--Exercise 2.1: Days of the Week
---------------------------------------------

print("\n\n\nEXERCISE 2.1")

-- initialize the days
days = {Monday = 19, Tuesday = 21, Wednsday = 18, Thursday = 17, Friday = 19, Saturday = 23, Sunday = 20}

lowest, highest, sum, count = days["Monday"], days["Monday"], 0, 0 

for key, value in pairs(days) do
	if value > highest then
		highest = value
        highest_day = key
	end
	if value < lowest then
		lowest = value
        lowest_day = key
	end
	sum = sum + value
	count = count + 1
end

average = sum / count

print("warmest: ", highest_day)
print("coldest: ", lowest_day)
print("average: ", average)


---------------------------------------------
--Exercise 2.2: Join
---------------------------------------------

print("\n\n\nEXERCISE 2.2")

t1 = {}
t1["C1966820"] = "John Doe"
t1["C1965463"] = "Homer Simpson"
t1["C1987900"] = "Jonnie Walker"
t1["C2345820"] = "Paul Smith"

t2 = {}
t2["John Doe"] = 26
t2["Homer Simpson"] = 40
t2["Jonnie Walker"] = 90
t2["Paul Smith"] = 30

function join_tables(t1, t2)
	t3 = {}
	for key1, value1 in pairs(t1) do
		if t2[value1] ~= nil then
			t3[key1] = t2[value1]
		end
	end
	return t3
end

for key, value in pairs(join_tables(t1, t2)) do
	print(key, value)
end


---------------------------------------------
--Exercise 2.3: Tables as set
---------------------------------------------

print("\n\n\nEXERCISE 2.3")

function createSetOfRandIntegers(set, numberOfElements)
	math.randomseed(os.time())
	count = 0
	while count < numberOfElements do
		value = math.random(0, 100)
		if set[value] == nil then
			set[value] = true
			count = count + 1
		end
	end
end

mySet = {}
createSetOfRandIntegers(mySet, 30)

for key, value in pairs(mySet) do
	print(key)
end


---------------------------------------------
--Exercise 2.4: Tables as Multiset
---------------------------------------------

print("\n\n\nEXERCISE 2.3")

function init_multiset(list) 
	result = {}
	for index, item in pairs(list) do
		if result[item] == nil then
			result[item] = 1
		else 
			result[item] = result[item] + 1
		end
	end
	return result
end

myList = {"apple", "peach", "lemon", "apple", "apple", "peach"}
multiset = init_multiset(myList)

function insert(multiset, element)
	if multiset[element] == nil then
		multiset[element] = 1
	else
		multiset[element] = multiset[element] + 1
	end
end

insert(multiset, "lemon")

function remove(multiset, element)
	if multiset[element] == nil then
		return
	elseif multiset[element] == 1 then
		multiset[element] = nil
	else
		multiset[element] = multiset[element] - 1
	end
end

remove(multiset, "lemon")
remove(multiset, "pineapple")
remove(multiset, "peach")
remove(multiset, "peach")

function is_elem(multiset, element) 
    result = multiset[element] ~= nil 
	return result
end

print(is_elem(multiset, "lemon"))
print(is_elem(multiset, "peach"))

function count_elem(multiset, element)
    if multiset[element] == nil then
        return 0
    else
        return multiset[element]
    end
end

print(count_elem(multiset, "lemon"))

for key, value in pairs(multiset) do
	print(key, value)
end




