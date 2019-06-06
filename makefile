CC = gcc
CFLAGS = -o 
LIBS = -lrabbitmq -lcjson 
all:
	$(CC) $(LIBS) $(CFLAGS) Nazca_v2v Nazca_v2v.c
clean:
	rm -f Nazca_v2v
