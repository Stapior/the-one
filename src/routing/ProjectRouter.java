package routing;

import core.Application;
import core.Connection;
import core.DTNHost;
import core.Message;
import core.MessageListener;
import core.Settings;
import core.SimClock;
import core.SimError;
import util.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static core.Constants.DEBUG;

public class ProjectRouter extends ActiveRouter {

    private static final int SENT_COUNT = 3;
    private static final int ROUTING_SIZE = 3;
    private static final String RESPONSE_ROUTING = "RR";
    private static final boolean RESEND_RESPONSE = true;

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
            List<DTNHost> responseRouting = (List<DTNHost>) message.getProperty(RESPONSE_ROUTING);
            if (responseRouting != null) {
                DTNHost next = responseRouting.get(responseRouting.size() - 1);
                Optional<Connection> connectionForNext = getConnectionForHost(next);
                if (connectionForNext.isPresent() &&
                        this.tryMessagesForConnected(Collections.singletonList(new Tuple<>(message, connectionForNext.get()))) != null) {
                    return;
                }
                continue;
            }


            List<DTNHost> routingToHost = getRoutingForMessage(message);
            for (int i = 1, orDefaultSize = routingToHost.size(); i <= orDefaultSize; i++) {
                DTNHost hostToSend = routingToHost.get(routingToHost.size() - i);
                Optional<Connection> connectionForHost = getConnectionForHost(hostToSend);
                if (connectionForHost.isPresent() && !message.getHops().contains(connectionForHost.get().getOtherNode(getHost()))) {
                    if (sendMessage(message, connectionForHost.get())) {
                        return;
                    }
                }
            }
        }

        for (Message message : this.getMessageCollection()) {
            List<DTNHost> messageSentHosts = this.messagesSents.get(message.getId());
            getConnections().stream().filter(connection -> messageSentHosts == null || !messageSentHosts.contains(connection.getOtherNode(getHost())))
                    .anyMatch(connection -> sendMessage(message, connection));
        }

    }


    private List<DTNHost> getRoutingForMessage(Message message) {
        DTNHost destination = message.getTo();
        return routingMap.getOrDefault(destination, new ArrayList<>());
    }

    private Optional<Connection> getConnectionForHost(DTNHost hostToSend) {
        return this.getConnections().stream().filter(con ->
                con.getOtherNode(getHost()).equals(hostToSend)).findFirst();
    }


    private boolean sendMessage(Message message, Connection connection) {
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
    protected void transferDone(Connection con) {
        Message m = con.getMessage();

        if (m == null) {
            if (DEBUG) core.Debug.p("Null message for con " + con);
            return;
        }

        messagesSents.putIfAbsent(m.getId(), new ArrayList<>());
        List<DTNHost> sent = messagesSents.get(m.getId());
        sent.add(con.getOtherNode(getHost()));

        if (m.getProperty(RESPONSE_ROUTING) != null || m.getTo() == con.getOtherNode(getHost()) || sent.size() >= SENT_COUNT) {
            this.deleteMessage(m.getId(), false);
        }

    }

    @Override
    public Message messageTransferred(String id, DTNHost from) {
        Message message = removeFromIncomingBuffer(id, from);

        if (message == null) {
            throw new SimError("No message with ID " + id + " in the incoming " +
                    "buffer of " + getHost());
        }

        message.setReceiveTime(SimClock.getTime());
        List<DTNHost> responseRouting = (List<DTNHost>) message.getProperty(RESPONSE_ROUTING);
        if (responseRouting == null) {
            grandParentMessageTransfered(id, from, message);
            addHopsToRouting(from, message);
            resendResponse(message);
        } else {
            addHopsToRouting(from, message);
            responseRouting.remove(getHost());
        }
        return message;
    }

    private void resendResponse(Message message) {
        if (message.getTo() == getHost() && RESEND_RESPONSE) {//                // generate a response message
            Message res = new Message(this.getHost(), message.getFrom(),
                    RESPONSE_PREFIX + message.getId(), 10);
            message.getHops().remove(message.getHops().size() - 1);
            res.updateProperty(RESPONSE_ROUTING, message.getHops());
            this.createNewMessage(res);
        }
    }

    @Override
    protected Message getNextMessageToRemove(boolean excludeMsgBeingSent) {
        Collection<Message> messages = this.getMessageCollection();
        Message oldest = null;
        for (Message m : messages) {
            if (excludeMsgBeingSent && isSending(m.getId())) {
                continue;
            }
            boolean oldestIsRR = oldest != null && oldest.getProperty(RESPONSE_ROUTING) != null;
            boolean mIsRR = m.getProperty(RESPONSE_ROUTING) != null;
            if (oldest == null || (!oldestIsRR && mIsRR)) {
                oldest = m;
                continue;
            }
            if (oldestIsRR && !mIsRR) {
                continue;
            }
            if (oldest.getReceiveTime() > m.getReceiveTime()) {
                oldest = m;
            }
        }
        return oldest;
    }

    private void grandParentMessageTransfered(String id, DTNHost from, Message message) {
        Message outgoing = message;
        for (Application app : getApplications(message.getAppID())) {
            outgoing = app.handle(outgoing, getHost());
            if (outgoing == null) break;
        }
        Message aMessage = (outgoing == null) ? (message) : (outgoing);
        boolean isFinalRecipient = aMessage.getTo() == getHost();
        boolean isFirstDelivery = isFinalRecipient &&
                !isDeliveredMessage(aMessage);

        if (!isFinalRecipient && outgoing != null) {
            addToMessages(aMessage, false);
        } else if (isFirstDelivery) {
            this.deliveredMessages.put(id, aMessage);
        } else if (outgoing == null) {
            this.blacklistedMessages.put(id, null);
        }

        for (MessageListener ml : this.mListeners) {
            ml.messageTransferred(aMessage, from, getHost(),
                    isFirstDelivery);
        }
    }

    private void addHopsToRouting(DTNHost from, Message message) {
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
    }

    @Override
    public void deleteMessage(String id, boolean drop) {
        super.deleteMessage(id, drop);
        messagesSents.remove(id);
    }

    @Override
    public ProjectRouter replicate() {
        return new ProjectRouter(this);
    }
}
