package servicebus;
//Include the following imports to use Service Bus APIs
import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.google.gson.Gson;

import static java.nio.charset.StandardCharsets.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.cli.*;

public class Sender {
	public void run() throws Exception {
		// Create a QueueClient instance and then asynchronously send messages.
		// Close the sender once the send operation is complete.
		QueueClient sendClient = new QueueClient(new ConnectionStringBuilder(ConnectionString, QueueName), ReceiveMode.PEEKLOCK);
		this.sendMessageAsync(sendClient).thenRunAsync(() -> sendClient.closeAsync());

		sendClient.close();
	}

	    CompletableFuture<Void> sendMessagesAsync(QueueClient sendClient) {
	        List<HashMap<String, String>> data =
	                GSON.fromJson(
	                        "[" +
	                                "{'name' = 'Einstein', 'firstName' = 'Albert'}," +
	                                "{'name' = 'Heisenberg', 'firstName' = 'Werner'}," +
	                                "{'name' = 'Curie', 'firstName' = 'Marie'}," +
	                                "{'name' = 'Hawking', 'firstName' = 'Steven'}," +
	                                "{'name' = 'Newton', 'firstName' = 'Isaac'}," +
	                                "{'name' = 'Bohr', 'firstName' = 'Niels'}," +
	                                "{'name' = 'Faraday', 'firstName' = 'Michael'}," +
	                                "{'name' = 'Galilei', 'firstName' = 'Galileo'}," +
	                                "{'name' = 'Kepler', 'firstName' = 'Johannes'}," +
	                                "{'name' = 'Kopernikus', 'firstName' = 'Nikolaus'}" +
	                                "]",
	                        new TypeToken<List<HashMap<String, String>>>() {}.getType());

	        List<CompletableFuture> tasks = new ArrayList<>();
	        for (int i = 0; i < data.size(); i++) {
	            final String messageId = Integer.toString(i);
	            Message message = new Message(GSON.toJson(data.get(i), Map.class).getBytes(UTF_8));
	            message.setContentType("application/json");
	            message.setLabel("Scientist");
	            message.setMessageId(messageId);
	            message.setTimeToLive(Duration.ofMinutes(2));
	            System.out.printf("\nMessage sending: Id = %s", message.getMessageId());
	            tasks.add(
	                    sendClient.sendAsync(message).thenRunAsync(() -> {
	                        System.out.printf("\n\tMessage acknowledged: Id = %s", message.getMessageId());
	                    }));
	        }
	        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[tasks.size()]));
	    }
}
