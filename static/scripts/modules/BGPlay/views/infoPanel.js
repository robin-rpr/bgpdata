import Mustache from "mustache";

const template = `
<div class="bgplayTitle">Info</div>
{{#node}}
    {{#node.id}}<div><b>Node ID:</b> {{node.id}}</div>{{/node.id}}
    {{#node.as}}<div><b>AS:</b> {{node.as}}</div>{{/node.as}}
    {{#node.owner}}<div><b>Owner:</b> {{node.owner}}</div>{{/node.owner}}
    {{#isASource}}
        <div><b>Collector peers:</b> {{#rrcPeers}}{{id}}  {{/rrcPeers}}</div>
    {{/isASource}}
    <!--{{#node.nodeUrl}}
        <div><b>Url:</b> <a href="http://bgp.potaroo.net{{node.nodeUrl}}">click</a></div>
    {{/node.nodeUrl}}-->
    {{#node.country}}<div><b>Country:</b> {{node.country}}</div>{{/node.country}}
{{/node}}

{{#path}}
    {{^node}}
        {{#path.source}}<div><b>Source:</b> {{path.source}}</div>{{/path.source}}
        {{#path.target}}<div><b>Target:</b> {{path.target}}</div>{{/path.target}}
        {{#pathString}}<div><b>Current Path:</b> {{pathString}}</div>{{/pathString}}
        {{#pathStatistics}}<div><b>Statistics:</b> {{pathStatistics}}</div>{{/pathStatistics}}
    {{/node}}
{{/path}}

{{^node}}
    {{^path}}

        {{^lambdas.isInitialState}}
            <div>
                {{#lastEvent.subType}}<b>Type:</b> {{lastEvent.type}} &gt; {{lastEvent.subType}}{{/lastEvent.subType}}
                {{#lastEvent.target}} <b>Involving:</b> {{lastEvent.target}}{{/lastEvent.target}}
            </div>
            {{#lastEvent.description}}<div><b>Short description:</b> {{lastEvent.description}}</div>{{/lastEvent.description}}
            <!--{{#lastEvent.path}}
                <div><b>Path:</b> {{lastEvent.path}}</div>
            {{/lastEvent.path}}-->
            {{#lastEvent.path}}
                <div>
                    <b>Path:</b>
                    {{#lastEvent.path.hops}}<a href="javascript:void(0);" class="bgplayAsLink">{{id}}</a>, {{/lastEvent.path.hops}}
                </div>
            {{/lastEvent.path}}
            {{#lastEvent.community}}<div><b>Community:</b> {{lastEvent.community}}</div>{{/lastEvent.community}}
            <div>
                {{#lastEvent.instant}}<b>Date and time:</b> {{lastEvent.instant.timestamp}}{{/lastEvent.instant}}
                {{#lastEvent.source}} <b>Collected by:</b> {{lastEvent.source}}{{/lastEvent.source}}
            </div>
        {{/lambdas.isInitialState}}

        {{#lambdas.isInitialState}}
            <table style="margin-top:25px;margin-left:-3px;">
                <tr>
                    <td>{{#lastEvent.type}}<b>Type:</b> Initial state{{/lastEvent.type}}</td>
                    <td><b>Number of ASes:</b> {{lambdas.nodesCount}}</td>
                </tr>
                <tr>
                    <td><b>Number of collector peers:</b> {{lambdas.sourcesCount}}</td>
                    <td><b>Selected RRCs:</b> {{_self.rrcs}}</td>
                </tr>
                <tr><td><b>Total number of events:</b> {{lambdas.eventsCount}}</td></tr>
                {{#lastEvent.instant}}<tr><td><b>Date and time:</b> {{lastEvent.instant.timestamp}}</td></tr>{{/lastEvent.instant}}
            </table>
        {{/lambdas.isInitialState}}
    {{/path}}
{{/node}}
`;

class InfoPanelView {
  blockEvents = false;

  constructor(self) {
    this._self = self;

    this.lastEvent = this._self.events.nearest(this._self.instant, false, true);

    this.lambdas = {
      isInitialState: () => this.lastEvent.type === "initialstate",
      nodesCount: () => this._self.nodes.size(),
      sourcesCount: () => this._self.sources.size(),
      eventsCount: () => this._self.events.size() - 1,
    };

    this.element = jQuery("<div>")
      .addClass("info-panel")
      .append(Mustache.render(template, this));

    this._initMessageListeners();
  }

  _initMessageListeners() {
    this._self.messages.on("nodeSelected", (nodeView) => {
      if (!this.blockEvents) {
        this.node = nodeView.node;
        this.rrcPeers = this._self.sources
          .filter((source) => source.as_number === nodeView.id)
          .map((source) => source.rrc);
        this.isASource = this.rrcPeers.length > 0;
        this.render();
      }
    });

    this._self.messages.on("pathSelected", (pathView) => {
      if (
        pathView.subtree === null &&
        pathView.path !== null &&
        !this.blockEvents
      ) {
        this.path = pathView.path;
        this.pathString = this.path.hops.join(",");
        this.pathStatistics = pathView.statistics;
      } else {
        this.path = null;
      }
      this.render();
    });

    this._self.messages.on("instant:changed", () => {
      this.lastEvent = this._self.events.nearest(
        this._self.instant,
        false,
        true,
      );

      if (this.lastEvent !== null) {
        this.lastEvent.isInitialState =
          this.lastEvent.subType === "initialstate";
        this.render();
      }
    });
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this._self.element.append(this.element);
  }
}

// Assuming that you want to export this class to be used in other files
export default InfoPanelView;
