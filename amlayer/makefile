a.out : am.o amfns.o amsearch.o aminsert.o ammodify.o amstack.o amglobals.o ambulk.o ../pflayer/pflayer.o main.o amscan.o amprint.o
	cc -g -w am.o amfns.o amsearch.o aminsert.o ammodify.o amstack.o amglobals.o ambulk.o -lm ../pflayer/pflayer.o main.o amscan.o amprint.o 

amlayer.o : am.o amfns.o amsearch.o aminsert.o ammodify.o amstack.o amglobals.o amscan.o amprint.o
	ld -r am.o amfns.o amsearch.o aminsert.o ammodify.o amstack.o amglobals.o amscan.o amprint.o  -o amlayer.o

am.o : am.c am.h pf.h
	cc -c am.c

amfns.o : amfns.c am.h pf.h
	cc -c amfns.c

amsearch.o : amsearch.c am.h pf.h
	cc -c amsearch.c

aminsert.o : aminsert.c am.h pf.h
	cc -c aminsert.c
	
ammodify.o : ammodify.c am.h pf.h
	cc -c ammodify.c

amscan.o : amscan.c am.h pf.h
	cc -c amscan.c

amstack.o : amstack.c am.h pf.h
	cc -c amstack.c

amglobals.o : amglobals.c am.h
	cc -c amglobals.c

amprint.o : amprint.c am.h pf.h 
	cc -c amprint.c
	
ambulk.o : ambulk.c
	cc -c ambulk.c -lm


main.o : main.c am.h pf.h 
	cc -c main.c

clean : 
	rm *.o
