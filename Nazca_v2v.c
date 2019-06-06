/*
INFO:                   Jest to testowy program realizujacy komunikacje dwustronna systemem Nazca
                        poprzez protokol AMQP pracuje on petli ciaglej, nie zwiera funkcji
                        zwalniajacych polaczenie, program zatrzymuje sie poprzez ctrl-break.

Realizowane zadania:	Program wykonuje kolejno
				Laczenie z brokerem AMQP RabbitMQ
				Tworzy kolejke
				Deklaruje exchange (InteropServices) ta nazwa jest istotna!!
				Binduje utworzona kolejke w ramach exchang'a na klucze routujace uzywane w zadaniu testowym
				Uruchamia konsumpcje wiadomosci z wyswietlaniem odpowiedzi w konsoli
				Przesyla komunikat typu tekstowego do Systemu Nazca na klucz coutry
				Przesyla komunikat typu logicznego do Systemu Nazca na klucz binary 2x
				Przesyla komunikat typu numerycznego do Systemu Nazca na klucz value


Uzywane Biblioteki:     Rabbitmq-c https://github.com/alanxz/rabbitmq-c
                        cJSON https://github.com/DaveGamble/cJSON/
						Program wymaga ich zainstalowania jako shared library
Kompilacja:		make all
*/

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <cjson/cJSON.h>

 char	*AMQP_HOST = "rabbitcloud.com";
 int	 AMQP_PORT = 5672;
 char	*AMQP_VHOST = "vhost";
 char	*AMQP_USER = "user";
 char	*AMQP_PASS = "pass";
 char	*AMQP_QUEUE = "moja_kolejka";
 char	*AMQP_EXCHANGE = "InteropServices";
 char	*AMQP_RKEY1 = "state";
 char   *AMQP_RKEY2 = "binarynot";
 char   *AMQP_RKEY3 = "valuetimestwo";
 char   *BROKER_SENDER_ID = "CProgram";
 
 amqp_connection_state_t conn;

void BrokerMessageSend(amqp_connection_state_t hconnection, char *key,  char *value, char *flavor, char *sender)
{
    char *JsonCString = 0;

    printf("\nWyslano\nKlucz: %s\nWartosc: %s\nTyp: %s\nNadawca: %s\n\n\n", key, value, flavor, sender);

    cJSON *BrokerJsonMessage = cJSON_CreateObject();

    if(!strcmp(flavor, "Bool")){
	if(!strcmp(value, "True")){
		cJSON_AddBoolToObject(BrokerJsonMessage, "Value", cJSON_True);
	}
	
	 if(!strcmp(value, "False")){
		cJSON_AddBoolToObject(BrokerJsonMessage, "Value", 0);
	}
    }else{
	cJSON_AddStringToObject(BrokerJsonMessage, "Value", value);
    }

    cJSON_AddStringToObject(BrokerJsonMessage, "Flavor", flavor);
    cJSON_AddStringToObject(BrokerJsonMessage, "Sender", sender);
    JsonCString = cJSON_Print(BrokerJsonMessage);
//	printf("wysylanie: %s\n\n\n", JsonCString);
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */
    amqp_basic_publish(hconnection, 1, amqp_cstring_bytes(AMQP_EXCHANGE),
                                    amqp_cstring_bytes(key), 0, 0,
                                    &props, amqp_cstring_bytes(JsonCString));

 cJSON_Delete(BrokerJsonMessage);
             

 return;
}

void BrokerMessageProc(amqp_connection_state_t hconnection, char *key, char *value, char* flavor, char *sender)
{
	if(!strcmp(sender, BROKER_SENDER_ID)) return; // ignore echo 
	
	printf("Otrzymano\n\nKlucz: %s\nWartosc: %s\nTyp: %s\nNadawca: %s\n\n\n", key, value, flavor, sender); 
 return;
}

int main(int argc, char const *const *argv) {

 int status;
 amqp_socket_t *socket = NULL;
 

 conn = amqp_new_connection();
 socket = amqp_tcp_socket_new(conn);
 status = amqp_socket_open(socket, AMQP_HOST, AMQP_PORT);
 amqp_login(conn, AMQP_VHOST, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, AMQP_USER, AMQP_PASS);
 amqp_channel_open(conn, 1);
 amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, amqp_cstring_bytes(AMQP_QUEUE), 0, 0, 0, 1, amqp_empty_table);
 amqp_exchange_declare(conn, 1, amqp_cstring_bytes(AMQP_EXCHANGE), amqp_cstring_bytes("direct"), 0, 0, 0, 0, amqp_empty_table);
 amqp_queue_bind(conn, 1, amqp_cstring_bytes(AMQP_QUEUE),  amqp_cstring_bytes(AMQP_EXCHANGE), amqp_cstring_bytes(AMQP_RKEY1), amqp_empty_table);
 amqp_queue_bind(conn, 1, amqp_cstring_bytes(AMQP_QUEUE), amqp_cstring_bytes(AMQP_EXCHANGE), amqp_cstring_bytes(AMQP_RKEY2), amqp_empty_table); 
 amqp_queue_bind(conn, 1, amqp_cstring_bytes(AMQP_QUEUE), amqp_cstring_bytes(AMQP_EXCHANGE), amqp_cstring_bytes(AMQP_RKEY3), amqp_empty_table); 
 amqp_basic_consume(conn, 1, amqp_cstring_bytes(AMQP_QUEUE), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

 amqp_frame_t frame;
 char *buf = 0;
 char *buf_key = 0;
 char *bval = 0;

 BrokerMessageSend(conn, "country", "Niemcy", "Text", BROKER_SENDER_ID);
 BrokerMessageSend(conn, "binary", "True", "Bool", BROKER_SENDER_ID);
 BrokerMessageSend(conn, "binary", "False", "Bool", BROKER_SENDER_ID);
 BrokerMessageSend(conn, "value", "400", "Num", BROKER_SENDER_ID);

 for(;;){
	amqp_rpc_reply_t ret;
        amqp_envelope_t envelope;

 	amqp_maybe_release_buffers(conn);
	ret = amqp_consume_message(conn, &envelope, NULL, 0);
        

	if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
        break;
      }
/*
      printf("Numer wiadomosci %u, exchange %.*s klucz %.*s\n",
             (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
             (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
             (char *)envelope.routing_key.bytes);

      if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        printf("Content-type: %.*s\n",
               (int)envelope.message.properties.content_type.len,
               (char *)envelope.message.properties.content_type.bytes);
      }
      printf("----\n");

	printf("wiadomosc: %.*s\n",
             (int)envelope.message.body.len, (char*)envelope.message.body.bytes);
*/
	buf = malloc(envelope.message.body.len+1);
	memset(buf, 0x00, envelope.message.body.len);
	memcpy(buf, envelope.message.body.bytes, envelope.message.body.len);
        buf_key = malloc(envelope.routing_key.len+1);
 	memset(buf_key, 0x00, envelope.routing_key.len);
	memcpy(buf_key,  envelope.routing_key.bytes, envelope.routing_key.len);
	buf[envelope.message.body.len] = '\0';
	buf_key[envelope.routing_key.len] = '\0';
	cJSON *BrokerMessage = cJSON_Parse(buf);

	cJSON *BrokerMessageFlavor = cJSON_GetObjectItemCaseSensitive(BrokerMessage, "Flavor");
	cJSON *BrokerMessageSender = cJSON_GetObjectItemCaseSensitive(BrokerMessage, "Sender");
	cJSON *BrokerMessageValue = cJSON_GetObjectItemCaseSensitive(BrokerMessage, "Value");
	if(!strcmp(BrokerMessageFlavor->valuestring, "Bool")){
		if(cJSON_IsTrue(BrokerMessageValue) == 1){
			bval = "True";
		}else{
			bval = "False";
		}
	}

	if(BrokerMessageValue->valuestring == 0){
		BrokerMessageProc(conn, buf_key, bval, BrokerMessageFlavor->valuestring, BrokerMessageSender->valuestring);
	}else{
		BrokerMessageProc(conn, buf_key,  BrokerMessageValue->valuestring, BrokerMessageFlavor->valuestring, BrokerMessageSender->valuestring);
	}

	cJSON_Delete(BrokerMessage);
	free(buf);
	free(buf_key);

        amqp_destroy_envelope(&envelope);
    
}
 

 return 0;
}
