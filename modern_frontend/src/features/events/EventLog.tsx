import { EventEntryDto } from "./api";

type Props = {
  events: EventEntryDto[] | undefined;
};

const levelColor: Record<EventEntryDto["level"], string> = {
  info: "text-green-400",
  warning: "text-yellow-400",
  error: "text-red-400",
};

export function EventLog({ events }: Props) {
  return (
    <div className="terminal-window h-full">
      <div className="terminal-header">
        <div className="terminal-dots">
          <span className="dot red" />
          <span className="dot yellow" />
          <span className="dot green" />
        </div>
        <div className="terminal-title">ec2-cli — bash — 80×24</div>
      </div>
      <div className="terminal-body scrollbar-thin">
        <div className="terminal-output space-y-2 text-sm">
          {events && events.length > 0 ? (
            events
              .slice()
              .reverse()
              .map((event) => (
                <div key={event.id}>
                  <span className="terminal-prompt">➜</span>{" "}
                  <span className="text-blue-400">~</span>{" "}
                  <span className="text-gray-400 text-xs">
                    {new Date(event.timestamp).toLocaleTimeString()}
                  </span>{" "}
                  <span className={levelColor[event.level]}>
                    [{event.level.toUpperCase()}]
                  </span>{" "}
                  <span>{event.message}</span>
                </div>
              ))
          ) : (
            <>
              <div>
                <span className="terminal-prompt">➜</span>{" "}
                <span className="text-blue-400">~</span>{" "}
                Ready. Awaiting commands...
              </div>
              <div>
                <span className="terminal-prompt">➜</span>{" "}
                <span className="text-blue-400">~</span>{" "}
                <span className="text-green-400">// Start by selecting an instance</span>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}


