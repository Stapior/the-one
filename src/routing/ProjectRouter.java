package routing;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;
import util.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static core.Constants.DEBUG;

public class ProjectRouter extends ActiveRouter {

    public static final int ROUTING_SIZE = 3;

    public ProjectRouter(Settings s) {
        super(s);
    }

    protected ProjectRouter(ProjectRouter r) {
        super(r);
    }

    private final Map<DTNHost, List<DTNHost>> routingMap = new HashMap<>();
    private final Map<String, List<DTNHost>> messagesSents = new HashMap<>();

    @Override
    public void update() {
        super.update();
        if (isTransferring() || !canStartTransfer()) {
            return;
        }
        if (exchangeDeliverableMessages() != null) {
            return;
        }

        this.sendMessageByRoutingMap();
    }

    private void sendMessageByRoutingMap() {
        for (Message message : this.getMessageCollection()) {
            DTNHost destination = message.getTo();

            List<DTNHost> orDefault = routingMap.getOrDefault(destination, new ArrayList<>());
            for (int i = 1, orDefaultSize = orDefault.size(); i <= orDefaultSize; i++) {
                DTNHost hostToSend = orDefault.get(orDefault.size() - i);
                Optional<Connection> first = this.getConnections().stream().filter(con ->
                        con.getOtherNode(getHost()).equals(hostToSend)).findFirst();
                if (first.isPresent() && !message.getHops().contains(first.get().getOtherNode(getHost())) & trySend(first, message)) {
                   return;
                }
            }
        }

        for (Message message : this.getMessageCollection()) {
            for (Connection connection : this.getConnections()) {
                if(trySend(connection, message)){
                    return;
                }
            }
        }
    }

    private boolean trySend(Optional<Connection> connection, Message message) {
        return connection.isPresent() && trySend(connection.get(), message);
    }

    private boolean trySend(Connection connection, Message message) {
        return this.tryMessagesForConnected(Collections.singletonList(new Tuple<>(message, connection))) != null;
    }

    @Override
    protected int checkReceiving(Message m, DTNHost from) {
        int recvCheck = super.checkReceiving(m, from);

        if (recvCheck == RCV_OK && m.getHops().contains(getHost())) {
            return DENIED_OLD;
        }
        return recvCheck;
    }

    @Override
    public void deleteMessage(String id, boolean drop) {
        super.deleteMessage(id, drop);
        messagesSents.remove(id);
    }

    @Override
    protected void transferDone(Connection con) {
        Message m = con.getMessage();
        if (m == null) {
            if (DEBUG) core.Debug.p("Null message for con " + con);
            return;
        }

        messagesSents.putIfAbsent(m.getId(), new ArrayList<>());
        messagesSents.get(m.getId()).add(con.getOtherNode(getHost()));

        if (messagesSents.size() >= ROUTING_SIZE || m.getTo() == con.getOtherNode(getHost())) {
            this.deleteMessage(m.getId(), false);
        }

    }

    @Override
    public Message messageTransferred(String id, DTNHost from) {
        Message message = super.messageTransferred(id, from);
        message.getHops().forEach(pathHost -> {
            List<DTNHost> routingHosts = this.routingMap.putIfAbsent(pathHost, new ArrayList<>());
            if (routingHosts == null) {
                routingHosts = this.routingMap.get(pathHost);
            }
            routingHosts.add(from);
            if (routingHosts.size() > ROUTING_SIZE) {
                routingHosts.remove(0);
            }
        });

        return message;
    }


    @Override
    public ProjectRouter replicate() {
        return new ProjectRouter(this);
    }
}
