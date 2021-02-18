package routing;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;
import util.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static core.Constants.DEBUG;

public class ProjectRouter extends ActiveRouter {

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
        List<Tuple<Message, Connection>> forTuples = new ArrayList<>();

        for (Message message : this.getMessageCollection()) {
            if (messagesSents.getOrDefault(message.getId(), new ArrayList<>()).size() <= 2) {
                DTNHost destination = message.getTo();
                boolean sent = false;
                List<DTNHost> orDefault = routingMap.getOrDefault(destination, new ArrayList<>());
                for (int i = 1, orDefaultSize = orDefault.size(); i <= orDefaultSize; i++) {
                    DTNHost hostToSend = orDefault.get(orDefault.size() - i);
                    Optional<Connection> first = this.getConnections().stream().filter(con ->
                            con.getOtherNode(getHost()).equals(hostToSend)).findFirst();
                    if (first.isPresent() && !message.getHops().contains(first.get().getOtherNode(getHost()))) {
                        forTuples.add(new Tuple<>(message, first.get()));
                        sent = true;
                        break;
                    }
                }
                if (!sent) {
                    for (Connection connection : this.getConnections()) {
                        forTuples.add(new Tuple<>(message, connection));
                    }
                }
            }
        }

        this.tryMessagesForConnected(forTuples);
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
        messagesSents.putIfAbsent(m.getId(), new ArrayList<>());
        messagesSents.get(m.getId()).add(con.getOtherNode(getHost()));

        if (m == null) {
            if (DEBUG) core.Debug.p("Null message for con " + con);
            return;
        }
        if (m.getTo() == con.getOtherNode(getHost())) {
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
            if (routingHosts.size() > 2) {
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
