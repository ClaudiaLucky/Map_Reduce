for i in {1..20}  
do
	go test -race -run 3B >> ans
done
