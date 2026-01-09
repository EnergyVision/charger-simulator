import {createRpcClient} from "@push-rpc/core"
import {wrapWebsocket} from "@push-rpc/websocket/dist/server"
import * as WebSocket from "ws"
import {log} from "./log"
import {createCentralSystemClient, createChargePointServer} from "./soap/ocppSoap"

export interface Config {
  defaultHeartbeatIntervalSec?: number
  chargePointVendor?: string
  chargePointModel?: string
  startDelayMs?: number
  stopDelayMs?: number
  keepAliveTimeoutMs?: number // set to null to disable pings
  meterValuesIntervalSec?: number

  centralSystemEndpoint: string
  chargerIdentity: string
  chargePointPort?: number

  connectorId: number
}

const defaultConfig: Partial<Config> = {
  defaultHeartbeatIntervalSec: 30,
  chargePointVendor: "Test",
  chargePointModel: "1",
  startDelayMs: 8 * 1000,
  stopDelayMs: 8 * 1000,
  keepAliveTimeoutMs: 50 * 1000,
  meterValuesIntervalSec: 20,
}

let ws: WebSocket

export class ChargerSimulator {
  constructor(config: Config) {
    this.config = {...defaultConfig, ...config}

    this.configurationKeys = [
      {key: "HeartBeatInterval", readonly: false, value: "" + config.defaultHeartbeatIntervalSec},
      {key: "ResetRetries", readonly: false, value: "1"},
      {key: "MeterValueSampleInterval", readonly: false, value: config.meterValuesIntervalSec},
    ]
  }

  public async start() {
    if (this.config.chargePointPort) {
      await createChargePointServer(this.chargePoint, this.config.chargePointPort)
      log.info(
        `Started SOAP Charge Point server at http://localhost:${this.config.chargePointPort}/`
      )

      this.centralSystem = await createCentralSystemClient(
        this.config.centralSystemEndpoint,
        this.config.chargerIdentity,
        `http://localhost:${this.config.chargePointPort}/`
      )
      log.info(`Will send messages to Central System at ${this.config.centralSystemEndpoint}`)
    } else {
      const {remote} = await createRpcClient(
        async () => {
          const clientCertHeader =
            "-----BEGIN%20CERTIFICATE-----%0AMIICMTCCAdigAwIBAgIUMFwiI5510scr/QfWd4WhxBX+0YswCgYIKoZIzj0EAwIw%0AYDELMAkGA1UEBhMCQkUxFjAUBgNVBAgMDUVhc3QtZmxhbmRlcnMxETAPBgNVBAoM%0ACEVub3ZhdGVzMSYwJAYDVQQDDB1Fbm92YXRlcyBDaGFyZ2VyIEludGVybWVkaWF0%0AZTAeFw0yNTAxMjcxMTIyMTNaFw0zNDEwMjcxMTIyMTNaMGIxCzAJBgNVBAYTAkJF%0AMRYwFAYDVQQIDA1FYXN0LWZsYW5kZXJzMRAwDgYDVQQHDAdMb2tlcmVuMREwDwYD%0AVQQKDAhFbm92YXRlczEWMBQGA1UEAwwNSTIzMjIwMjM5NDU0NDBZMBMGByqGSM49%0AAgEGCCqGSM49AwEHA0IABFIEelLUWYWkL2D5Pm4usBiIv8VZ9Se5g2eefh4orEIj%0AH0r4C5PZC8LzM+9kFih9ig+hSaXfTD1hfUAVBJ+YjHWjbjBsMAsGA1UdDwQEAwIF%0AoDAdBgNVHSUEFjAUBggrBgEFBQcDAgYIKwYBBQUHAwgwHQYDVR0OBBYEFN6Z7JyR%0AwJos2aPXYVJFym6NGNH6MB8GA1UdIwQYMBaAFNWH6uyPSBRgCd0K+QEQT6Wa6SOA%0AMAoGCCqGSM49BAMCA0cAMEQCIHtIFBY4f26HHCUeojyJ25ormBRgyKNpjQmp8lor%0AfpWgAiASdBoNO1G1J8lKKHGmAOoG/zPDk58p8dzxXp0NnUm2/w==%0A-----END%20CERTIFICATE-----%0A-----BEGIN%20CERTIFICATE-----%0AMIICGjCCAcCgAwIBAgIBATAKBggqhkjOPQQDAjBlMQswCQYDVQQGEwJCRTEWMBQG%0AA1UECAwNRWFzdC1mbGFuZGVyczEQMA4GA1UEBwwHTG9rZXJlbjERMA8GA1UECgwI%0ARW5vdmF0ZXMxGTAXBgNVBAMMEEVub3ZhdGVzIFJvb3QgQ0EwHhcNMjMxMTA2MDgw%0AMzM3WhcNNDMxMTAxMDgwMzM3WjBgMQswCQYDVQQGEwJCRTEWMBQGA1UECAwNRWFz%0AdC1mbGFuZGVyczERMA8GA1UECgwIRW5vdmF0ZXMxJjAkBgNVBAMMHUVub3ZhdGVz%0AIENoYXJnZXIgSW50ZXJtZWRpYXRlMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE%0A27N/DsCKJPDH5DATjUQ24UlzZuUGJxOxeZ8G8Xi6YHGPmLt35LgBVuTBetpLE+oE%0Ait/lf+wCP90TIv6qiYCVWKNmMGQwHQYDVR0OBBYEFNWH6uyPSBRgCd0K+QEQT6Wa%0A6SOAMB8GA1UdIwQYMBaAFFzaBVE16vBVkfqyqNMbdB3rf15xMBIGA1UdEwEB/wQI%0AMAYBAf8CAQAwDgYDVR0PAQH/BAQDAgGGMAoGCCqGSM49BAMCA0gAMEUCIBVqFypZ%0AiqoOh3kdmBmc05Bgc9VfqSOEF7IG96H5+0snAiEA1mHPYczxEBFMvgWEd8hr/zC5%0AImb4IX8tL0vYJhrugRY=%0A-----END%20CERTIFICATE-----%0A"
          const wsOptions = {headers: {"x-amzn-mtls-clientcert": clientCertHeader}}

          ws = new WebSocket(
            this.config.centralSystemEndpoint + "/" + this.config.chargerIdentity,
            "ocpp1.6",
            wsOptions
          )

          return wrapWebsocket(ws)
        },
        {
          local: this.chargePoint,
          reconnect: true,
          keepAliveTimeout: this.config.keepAliveTimeoutMs,

          listeners: {
            messageIn: (data) => {
              log.debug("OCPP in", data)
            },
            messageOut: (data) => {
              log.debug("OCPP out", data)
            },
            connected() {
              log.debug("OCPP connected")
            },
            disconnected({code, reason}) {
              log.debug("OCPP disconnected", {code, reason})
            },
            subscribed(subscriptions: number): void {},
            unsubscribed(subscriptions: number): void {},
          },
        }
      )

      log.info(
        `Connected to Central System at ${this.config.centralSystemEndpoint} using WebSocket`
      )

      this.centralSystem = remote
    }

    if (this.config.defaultHeartbeatIntervalSec) {
      setInterval(() => {
        this.centralSystem.Heartbeat()
      }, this.config.defaultHeartbeatIntervalSec * 1000)
    }
  }

  public startTransaction({connectorId, idTag}, delay) {
    if (this.meterTimer) {
      return false
    }

    setTimeout(
      async () => {
        this.transactionId = (
          await this.centralSystem.StartTransaction({
            connectorId,
            idTag,
            timestamp: new Date(),
            meterStart: 0,
          })
        ).transactionId

        this.charged = 0

        this.meterTimer = setInterval(() => {
          this.charged += Math.random() > 0.66 ? 30 : 20 // 26.6 W / 10s avg = 9.36 Kw

          this.centralSystem.MeterValues({
            connectorId,
            transactionId: this.transactionId,
            meterValue: [
              {
                timestamp: new Date(),
                sampledValue: [
                  {
                    value: "" + this.charged,
                    measurand: "Energy.Active.Import.Register",
                    unit: "Wh",
                  },
                  {
                    value: "38",
                    measurand: "SoC",
                    unit: "Percent",
                  },
                ],
              },
            ],
          })
        }, this.config.meterValuesIntervalSec * 1000)
      },
      delay ? this.config.startDelayMs : 0
    )

    return true
  }

  public stopTransaction(delay) {
    if (!this.meterTimer) {
      return false
    }

    clearInterval(this.meterTimer)

    setTimeout(
      async () => {
        await this.centralSystem.StopTransaction({
          transactionId: this.transactionId,
          timestamp: new Date(),
          meterStop: this.charged,
        })

        this.meterTimer = null
        this.transactionId = null
      },
      delay ? this.config.stopDelayMs : 0
    )

    return true
  }

  disconnect() {
    ws.close()
  }

  public centralSystem = null

  private config: Config = null
  private meterTimer = null
  private charged = 0
  private configurationKeys = []
  private transactionId = null
  public chargePoint = {
    RemoteStartTransaction: async (req) => {
      if (!req.connectorId) {
        req.connectorId = this.config.connectorId
      }

      return {
        status: this.startTransaction(req, true) ? "Accepted" : "Rejected",
        // status: "Rejected",
      }
    },

    RemoteStopTransaction: async (req) => {
      return {
        status: this.stopTransaction(true) ? "Accepted" : "Rejected",
      }
    },

    GetConfiguration: async (req) => {
      await new Promise((r) => setTimeout(r, 2000))

      return {configurationKey: this.configurationKeys}
    },
    ChangeConfiguration: async (req) => {
      for (let i = 0; i < this.configurationKeys.length; i++) {
        if (this.configurationKeys[i].key == req.key) {
          this.configurationKeys[i].value = "" + req.value
        }
      }

      return {status: "Accepted"}
    },

    ChangeAvailability: async (req) => {
      this.chargePoint.currentConnectorStatus = req.type
      this.chargePoint.currentConnectorId = req.connectorId

      return {status: "Accepted"}
    },

    ClearCache: async (req) => {
      return {status: "Accepted"}
    },

    ReserveNow: async (req) => {
      return {status: "Accepted"}
    },

    CancelReservation: async (req) => {
      return {status: "Accepted"}
    },

    Reset: async (req) => {
      return {status: "Accepted"}
    },

    currentConnectorId: 1,
    currentConnectorStatus: "Available",
    TriggerMessage: async (req) => {
      if ("BootNotification" === req.requestedMessage) {
        this.centralSystem.BootNotification({
          chargePointVendor: "OC",
          chargePointModel: "OCX",
        })
      }

      if ("StatusNotification" === req.requestedMessage) {
        let status = "Available"
        if (this.chargePoint.currentConnectorStatus == "Operative") {
          status = "Available"
        } else {
          status = "Unavailable"
        }

        this.centralSystem.StatusNotification({
          connectorId: this.chargePoint.currentConnectorId,
          errorCode: "NoError",
          status: status,
        })
      }

      return {status: "Accepted"}
    },

    UpdateFirmware: async (req) => {
      return {status: "Accepted"}
    },

    UnlockConnector: async (req) => {
      return {status: "Unlocked"}
    },
    GetDiagnostics: async (req) => {
      return {fileName: "file.extension"}
    },
    SendLocalList: async (req) => {
      return {status: "Accepted"}
    },
  }
}
