package routing;

import java.util.Arrays;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;

public class AdamRouter extends ActiveRouter {

	public AdamRouter(Settings s) {
		super(s);
	}

	protected AdamRouter(AdamRouter r) {
		super(r);
	}

	@Override
	public void update() {
		super.update();
		if (isTransferring() || !canStartTransfer()) {
			return;
		}

		if (exchangeDeliverableMessages() != null) {
			return;
		}

		Message messageToRemove = this.getNextMessageToRemove(false);
		DTNHost aHost = this.getHost();
		DTNHost endingHost = messageToRemove.getTo();
		Connection designatedConnection = null;

		for (Connection connection : this.getConnections()) {
			if(connection.getOtherNode(aHost).equals(endingHost)){
				designatedConnection = connection;
				break;
			};
		}

		if (designatedConnection != null) {
			this.tryMessagesToConnections(Arrays.asList(messageToRemove), Arrays.asList(designatedConnection));
		} else {
			this.tryAllMessagesToAllConnections();
		}
	}

	@Override
	public AdamRouter replicate() {
		return new AdamRouter(this);
	}
}
