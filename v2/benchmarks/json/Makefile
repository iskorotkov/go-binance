run:
	go test -run="^$$" -bench=. -count=10 -benchmem > results.txt
	benchstat results.txt > benchstat.txt
