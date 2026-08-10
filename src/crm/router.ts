import express, { type Request, type Response } from "express";
import { isCrmRequestAuthorized } from "./auth";
import { PostgresCrmRepository } from "./repository";
import {
  CampaignWorkbench,
  CampaignWorkbenchError,
  CampaignWorkbenchNotFoundError,
} from "./campaign-workbench";
import {
  CrmGateViolationError,
  CrmNotFoundError,
  GovernedCrmService,
  isApprovalDecision,
  isApprovalGate,
} from "./service";

export interface CrmRouterOptions {
  service: GovernedCrmService;
  campaignWorkbench?: CampaignWorkbench;
  apiToken: string | undefined;
  databaseConfigured: boolean;
}

function campaignActor(req: Request): string {
  // Only the server-side CRM bearer client receives this API token. The portal
  // forwards its Clerk-verified staff identity in this header; browser clients
  // never receive the token.
  const claimed = requestedText(req.headers["x-crm-actor"]);
  return claimed?.trim().slice(0, 256) || "crm_api_token";
}

function campaignError(res: Response, error: unknown): Response | null {
  if (error instanceof CampaignWorkbenchNotFoundError)
    return res.status(404).json({ error: error.message });
  if (error instanceof CampaignWorkbenchError)
    return res.status(409).json({ error: error.message });
  return null;
}

function validationError(res: Response, message: string): Response {
  return res.status(400).json({ error: message });
}

function requestedText(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

export function createCrmRouter(options: CrmRouterOptions): express.Router {
  const router = express.Router();

  router.use((req, res, next) => {
    if (!isCrmRequestAuthorized(req, options.apiToken)) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    if (!options.databaseConfigured && req.path !== "/health") {
      return res.status(503).json({ error: "CRM database is not configured" });
    }
    return next();
  });

  router.get("/health", async (_req, res) => {
    const health = await options.service.health(options.databaseConfigured);
    return res.status(health.ready ? 200 : 503).json(health);
  });

  router.get("/accounts", async (req: Request, res: Response) => {
    const requestedLimit = Number.parseInt(
      requestedText(req.query.limit) ?? "100",
      10,
    );
    const accounts = await options.service.listAccounts(
      Number.isFinite(requestedLimit) ? requestedLimit : 100,
    );
    return res
      .status(200)
      .json({ accounts, freshness: { checked_at: new Date().toISOString() } });
  });

  router.get("/stations/overview", async (req: Request, res: Response) => {
    const requestedLimit = Number.parseInt(
      requestedText(req.query.limit) ?? "50",
      10,
    );
    const overview = await options.service.getStationOverview(
      Number.isFinite(requestedLimit) ? requestedLimit : 50,
    );
    return res.status(200).json({
      stations: overview,
      governance: {
        mode: "read_only",
        approvals:
          "No transition is performed by this endpoint. Use POST /crm/approvals for a reviewed gate decision.",
      },
      freshness: { checked_at: new Date().toISOString() },
    });
  });

  router.post("/accounts", async (req: Request, res: Response) => {
    const source = requestedText(req.body?.source);
    if (!source?.trim()) return validationError(res, "source is required");
    if (
      [
        "account_approval",
        "contact_approval",
        "draft_approval",
        "send_approval",
      ].some((field) => field in (req.body ?? {}))
    ) {
      return validationError(
        res,
        "approval states are set only through /crm/approvals",
      );
    }
    try {
      const account = await options.service.createSourceIntakeAccount({
        source,
        displayName: requestedText(req.body?.display_name),
        externalReference: requestedText(req.body?.external_reference),
      });
      return res.status(201).json({ account });
    } catch (error) {
      if (error instanceof CrmGateViolationError)
        return validationError(res, error.message);
      throw error;
    }
  });

  router.post("/approvals", async (req: Request, res: Response) => {
    const accountId = requestedText(req.body?.account_id);
    const gate = req.body?.gate;
    const decision = req.body?.decision;
    if (!accountId || !isApprovalGate(gate) || !isApprovalDecision(decision)) {
      return validationError(
        res,
        "account_id, gate, and decision are required",
      );
    }
    try {
      // A single service token has no human identity claim. Record a stable, non-forgeable
      // service principal rather than accepting caller-supplied audit attribution.
      await options.service.recordApproval({
        accountId,
        gate,
        decision,
        actor: "crm_api_token",
      });
      return res
        .status(201)
        .json({ account_id: accountId, gate, decision, recorded: true });
    } catch (error) {
      if (error instanceof CrmNotFoundError)
        return res.status(404).json({ error: error.message });
      if (error instanceof CrmGateViolationError)
        return res.status(409).json({ error: error.message });
      throw error;
    }
  });

  // Manual-only campaign workbench. These routes never call a provider or send a message.
  router.get("/campaign-workbench", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    return res.status(200).json(await options.campaignWorkbench.overview());
  });
  router.get("/accounts/:accountId/workspace", async (req, res) => {
    if (!options.campaignWorkbench)
      return res.status(503).json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(200).json(await options.campaignWorkbench.accountWorkspace(req.params.accountId));
    } catch (error) {
      return campaignError(res, error) ?? (() => { throw error; })();
    }
  });
  router.post("/accounts/:accountId/notes", async (req, res) => {
    if (!options.campaignWorkbench)
      return res.status(503).json({ error: "Campaign workbench is not configured" });
    const note = requestedText(req.body?.note);
    if (!note?.trim()) return validationError(res, "note is required");
    try {
      return res.status(201).json({ note: await options.campaignWorkbench.addAccountNote(req.params.accountId, note, campaignActor(req)) });
    } catch (error) {
      return campaignError(res, error) ?? (() => { throw error; })();
    }
  });
  router.post("/contacts", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        contact: await options.campaignWorkbench.createContact(
          req.body ?? {},
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/contacts/:contactId/approval", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        contact: await options.campaignWorkbench.decideContact(
          req.params.contactId,
          req.body?.decision,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/contacts/:contactId/disposition", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(200).json({
        contact: await options.campaignWorkbench.setDisposition(
          req.params.contactId,
          req.body?.disposition,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/campaigns", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        campaign: await options.campaignWorkbench.createCampaign(
          req.body ?? {},
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/campaigns/:campaignId/members", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    const contactId = requestedText(req.body?.contact_id);
    if (!contactId) return validationError(res, "contact_id is required");
    try {
      return res.status(201).json({
        membership: await options.campaignWorkbench.addMember(
          req.params.campaignId,
          contactId,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/memberships/:membershipId/drafts", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        draft: await options.campaignWorkbench.createDraft(
          req.params.membershipId,
          req.body?.content,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/drafts/:draftId/approval", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        draft: await options.campaignWorkbench.decideDraft(
          req.params.draftId,
          req.body?.decision,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/memberships/:membershipId/attempts", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    const draftId = requestedText(req.body?.draft_id);
    if (!draftId) return validationError(res, "draft_id is required");
    try {
      return res.status(201).json({
        attempt: await options.campaignWorkbench.createAttempt(
          req.params.membershipId,
          draftId,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/attempts/:attemptId/approval", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        attempt: await options.campaignWorkbench.decideAttempt(
          req.params.attemptId,
          req.body?.decision,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });
  router.post("/attempts/:attemptId/manual-execution", async (req, res) => {
    if (!options.campaignWorkbench)
      return res
        .status(503)
        .json({ error: "Campaign workbench is not configured" });
    try {
      return res.status(201).json({
        attempt: await options.campaignWorkbench.recordManualExecution(
          req.params.attemptId,
          req.body?.outcome_note,
          campaignActor(req),
        ),
      });
    } catch (error) {
      return (
        campaignError(res, error) ??
        (() => {
          throw error;
        })()
      );
    }
  });

  return router;
}

/** Creates the independently configured CRM service. This intentionally never reads DATABASE_URL. */
export function createCrmRouterFromEnvironment(): express.Router {
  const connectionString = process.env.CRM_DATABASE_URL;
  const repository = PostgresCrmRepository.fromConnectionString(
    connectionString ?? "",
  );
  return createCrmRouter({
    service: new GovernedCrmService(repository),
    campaignWorkbench: connectionString
      ? CampaignWorkbench.fromConnectionString(connectionString)
      : undefined,
    apiToken: process.env.CRM_API_TOKEN,
    databaseConfigured: Boolean(connectionString),
  });
}
