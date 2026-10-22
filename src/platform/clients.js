'use strict';

/**
 * One client for each API the platform calls, created once per process on the shared mesh channel.
 */

const grpc = require('@grpc/grpc-js');
const { acme } = require('./protos');

const mesh = process.env.GRPC_MESH ?? '127.0.0.1:15001';
const insecure = grpc.credentials.createInsecure();

const accountingChartOfAccountsQueryClient = new acme.accounting.v1.ChartOfAccountsQueryService(mesh, insecure);
const admissionsAdmissionApprovalClient = new acme.admissions.v1.AdmissionApprovalService(mesh, insecure);
const admissionsAdmissionCalculationClient = new acme.admissions.v1.AdmissionCalculationService(mesh, insecure);
const admissionsAdmissionLifecycleClient = new acme.admissions.v1.AdmissionLifecycleService(mesh, insecure);
const admissionsAdmissionQueryClient = new acme.admissions.v1.AdmissionQueryService(mesh, insecure);
const admissionsAdmissionClient = new acme.admissions.v1.AdmissionService(mesh, insecure);
const admissionsAdmissionValidationClient = new acme.admissions.v1.AdmissionValidationService(mesh, insecure);
const admissionsAdmissionWorkflowClient = new acme.admissions.v1.AdmissionWorkflowService(mesh, insecure);
const admissionsGateScanApprovalClient = new acme.admissions.v1.GateScanApprovalService(mesh, insecure);
const admissionsGateScanCalculationClient = new acme.admissions.v1.GateScanCalculationService(mesh, insecure);
const admissionsGateScanLifecycleClient = new acme.admissions.v1.GateScanLifecycleService(mesh, insecure);
const admissionsGateScanQueryClient = new acme.admissions.v1.GateScanQueryService(mesh, insecure);
const admissionsGateScanValidationClient = new acme.admissions.v1.GateScanValidationService(mesh, insecure);
const admissionsGateScanWorkflowClient = new acme.admissions.v1.GateScanWorkflowService(mesh, insecure);
const admissionsWristbandApprovalClient = new acme.admissions.v1.WristbandApprovalService(mesh, insecure);
const admissionsWristbandCalculationClient = new acme.admissions.v1.WristbandCalculationService(mesh, insecure);
const admissionsWristbandLifecycleClient = new acme.admissions.v1.WristbandLifecycleService(mesh, insecure);
const admissionsWristbandReportingClient = new acme.admissions.v1.WristbandReportingService(mesh, insecure);
const admissionsWristbandValidationClient = new acme.admissions.v1.WristbandValidationService(mesh, insecure);
const admissionsWristbandWorkflowClient = new acme.admissions.v1.WristbandWorkflowService(mesh, insecure);
const analyticsAnalyticsEventQueryClient = new acme.analytics.v1.AnalyticsEventQueryService(mesh, insecure);
const anomaliesAnomalyQueryClient = new acme.anomalies.v1.AnomalyQueryService(mesh, insecure);
const anomaliesAnomalyClient = new acme.anomalies.v1.AnomalyService(mesh, insecure);
const assetsFixedAssetCalculationClient = new acme.assets.v1.FixedAssetCalculationService(mesh, insecure);
const auditAuditEventQueryClient = new acme.audit.v1.AuditEventQueryService(mesh, insecure);
const auditAuditEventClient = new acme.audit.v1.AuditEventService(mesh, insecure);
const auditAuditEventValidationClient = new acme.audit.v1.AuditEventValidationService(mesh, insecure);
const authorizationAccessPolicyQueryClient = new acme.authorization.v1.AccessPolicyQueryService(mesh, insecure);
const authorizationAccessPolicyClient = new acme.authorization.v1.AccessPolicyService(mesh, insecure);
const authorizationAccessPolicyValidationClient = new acme.authorization.v1.AccessPolicyValidationService(mesh, insecure);
const benefitsBenefitPlanQueryClient = new acme.benefits.v1.BenefitPlanQueryService(mesh, insecure);
const billingBillingAccountQueryClient = new acme.billing.v1.BillingAccountQueryService(mesh, insecure);
const boxofficeBoxOfficeSaleApprovalClient = new acme.boxoffice.v1.BoxOfficeSaleApprovalService(mesh, insecure);
const boxofficeBoxOfficeSaleCalculationClient = new acme.boxoffice.v1.BoxOfficeSaleCalculationService(mesh, insecure);
const boxofficeBoxOfficeSaleQueryClient = new acme.boxoffice.v1.BoxOfficeSaleQueryService(mesh, insecure);
const boxofficeBoxOfficeSaleClient = new acme.boxoffice.v1.BoxOfficeSaleService(mesh, insecure);
const boxofficeBoxOfficeSaleValidationClient = new acme.boxoffice.v1.BoxOfficeSaleValidationService(mesh, insecure);
const boxofficeBoxOfficeSaleWorkflowClient = new acme.boxoffice.v1.BoxOfficeSaleWorkflowService(mesh, insecure);
const boxofficeCashDrawerApprovalClient = new acme.boxoffice.v1.CashDrawerApprovalService(mesh, insecure);
const boxofficeCashDrawerCalculationClient = new acme.boxoffice.v1.CashDrawerCalculationService(mesh, insecure);
const boxofficeCashDrawerLifecycleClient = new acme.boxoffice.v1.CashDrawerLifecycleService(mesh, insecure);
const boxofficeCashDrawerQueryClient = new acme.boxoffice.v1.CashDrawerQueryService(mesh, insecure);
const boxofficeCashDrawerValidationClient = new acme.boxoffice.v1.CashDrawerValidationService(mesh, insecure);
const boxofficeCashDrawerWorkflowClient = new acme.boxoffice.v1.CashDrawerWorkflowService(mesh, insecure);
const boxofficeGuestListApprovalClient = new acme.boxoffice.v1.GuestListApprovalService(mesh, insecure);
const boxofficeGuestListCalculationClient = new acme.boxoffice.v1.GuestListCalculationService(mesh, insecure);
const boxofficeGuestListLifecycleClient = new acme.boxoffice.v1.GuestListLifecycleService(mesh, insecure);
const boxofficeGuestListReportingClient = new acme.boxoffice.v1.GuestListReportingService(mesh, insecure);
const boxofficeGuestListValidationClient = new acme.boxoffice.v1.GuestListValidationService(mesh, insecure);
const broadcastsBroadcastApprovalClient = new acme.broadcasts.v1.BroadcastApprovalService(mesh, insecure);
const broadcastsBroadcastQueryClient = new acme.broadcasts.v1.BroadcastQueryService(mesh, insecure);
const broadcastsBroadcastClient = new acme.broadcasts.v1.BroadcastService(mesh, insecure);
const broadcastsBroadcastValidationClient = new acme.broadcasts.v1.BroadcastValidationService(mesh, insecure);
const broadcastsBroadcastWorkflowClient = new acme.broadcasts.v1.BroadcastWorkflowService(mesh, insecure);
const broadcastsReplayClipApprovalClient = new acme.broadcasts.v1.ReplayClipApprovalService(mesh, insecure);
const broadcastsReplayClipCalculationClient = new acme.broadcasts.v1.ReplayClipCalculationService(mesh, insecure);
const broadcastsReplayClipLifecycleClient = new acme.broadcasts.v1.ReplayClipLifecycleService(mesh, insecure);
const broadcastsReplayClipValidationClient = new acme.broadcasts.v1.ReplayClipValidationService(mesh, insecure);
const broadcastsReplayClipWorkflowClient = new acme.broadcasts.v1.ReplayClipWorkflowService(mesh, insecure);
const broadcastsStreamKeyApprovalClient = new acme.broadcasts.v1.StreamKeyApprovalService(mesh, insecure);
const broadcastsStreamKeyCalculationClient = new acme.broadcasts.v1.StreamKeyCalculationService(mesh, insecure);
const broadcastsStreamKeyQueryClient = new acme.broadcasts.v1.StreamKeyQueryService(mesh, insecure);
const broadcastsStreamKeyValidationClient = new acme.broadcasts.v1.StreamKeyValidationService(mesh, insecure);
const broadcastsStreamKeyWorkflowClient = new acme.broadcasts.v1.StreamKeyWorkflowService(mesh, insecure);
const budgetingBudgetQueryClient = new acme.budgeting.v1.BudgetQueryService(mesh, insecure);
const campaignsMarketingCampaignQueryClient = new acme.campaigns.v1.MarketingCampaignQueryService(mesh, insecure);
const campaignsMarketingCampaignClient = new acme.campaigns.v1.MarketingCampaignService(mesh, insecure);
const catalogProductClient = new acme.catalog.v1.ProductService(mesh, insecure);
const chatChatRoomClient = new acme.chat.v1.ChatRoomService(mesh, insecure);
const collectionsCollectionCaseClient = new acme.collections.v1.CollectionCaseService(mesh, insecure);
const concessionsConcessionOrderApprovalClient = new acme.concessions.v1.ConcessionOrderApprovalService(mesh, insecure);
const concessionsConcessionOrderCalculationClient = new acme.concessions.v1.ConcessionOrderCalculationService(mesh, insecure);
const concessionsConcessionOrderLifecycleClient = new acme.concessions.v1.ConcessionOrderLifecycleService(mesh, insecure);
const concessionsConcessionOrderQueryClient = new acme.concessions.v1.ConcessionOrderQueryService(mesh, insecure);
const concessionsConcessionOrderReportingClient = new acme.concessions.v1.ConcessionOrderReportingService(mesh, insecure);
const concessionsConcessionOrderValidationClient = new acme.concessions.v1.ConcessionOrderValidationService(mesh, insecure);
const concessionsConcessionOrderWorkflowClient = new acme.concessions.v1.ConcessionOrderWorkflowService(mesh, insecure);
const concessionsConcessionStandApprovalClient = new acme.concessions.v1.ConcessionStandApprovalService(mesh, insecure);
const concessionsConcessionStandCalculationClient = new acme.concessions.v1.ConcessionStandCalculationService(mesh, insecure);
const concessionsConcessionStandLifecycleClient = new acme.concessions.v1.ConcessionStandLifecycleService(mesh, insecure);
const concessionsConcessionStandQueryClient = new acme.concessions.v1.ConcessionStandQueryService(mesh, insecure);
const concessionsConcessionStandClient = new acme.concessions.v1.ConcessionStandService(mesh, insecure);
const concessionsConcessionStandValidationClient = new acme.concessions.v1.ConcessionStandValidationService(mesh, insecure);
const concessionsConcessionStandWorkflowClient = new acme.concessions.v1.ConcessionStandWorkflowService(mesh, insecure);
const concessionsMenuItemApprovalClient = new acme.concessions.v1.MenuItemApprovalService(mesh, insecure);
const concessionsMenuItemCalculationClient = new acme.concessions.v1.MenuItemCalculationService(mesh, insecure);
const concessionsMenuItemLifecycleClient = new acme.concessions.v1.MenuItemLifecycleService(mesh, insecure);
const concessionsMenuItemReportingClient = new acme.concessions.v1.MenuItemReportingService(mesh, insecure);
const concessionsMenuItemValidationClient = new acme.concessions.v1.MenuItemValidationService(mesh, insecure);
const concessionsMenuItemWorkflowClient = new acme.concessions.v1.MenuItemWorkflowService(mesh, insecure);
const configurationConfigSetQueryClient = new acme.configuration.v1.ConfigSetQueryService(mesh, insecure);
const contentArticleClient = new acme.content.v1.ArticleService(mesh, insecure);
const contractsContractQueryClient = new acme.contracts.v1.ContractQueryService(mesh, insecure);
const corporateeventsCorporateEventCalculationClient = new acme.corporateevents.v1.CorporateEventCalculationService(mesh, insecure);
const corporateeventsCorporateEventQueryClient = new acme.corporateevents.v1.CorporateEventQueryService(mesh, insecure);
const customersCustomerCalculationClient = new acme.customers.v1.CustomerCalculationService(mesh, insecure);
const devicesManagedDeviceCalculationClient = new acme.devices.v1.ManagedDeviceCalculationService(mesh, insecure);
const documentsDocumentQueryClient = new acme.documents.v1.DocumentQueryService(mesh, insecure);
const documentsDocumentClient = new acme.documents.v1.DocumentService(mesh, insecure);
const documentsDocumentValidationClient = new acme.documents.v1.DocumentValidationService(mesh, insecure);
const expensesExpenseReportCalculationClient = new acme.expenses.v1.ExpenseReportCalculationService(mesh, insecure);
const expensesExpenseReportClient = new acme.expenses.v1.ExpenseReportService(mesh, insecure);
const fanportalEventPageNotificationClient = new acme.fanportal.v1.EventPageNotificationService(mesh, insecure);
const fanportalEventPageQueryClient = new acme.fanportal.v1.EventPageQueryService(mesh, insecure);
const fanportalEventPageClient = new acme.fanportal.v1.EventPageService(mesh, insecure);
const fanportalFanSessionQueryClient = new acme.fanportal.v1.FanSessionQueryService(mesh, insecure);
const fanportalFanSessionClient = new acme.fanportal.v1.FanSessionService(mesh, insecure);
const fanportalWishlistNotificationClient = new acme.fanportal.v1.WishlistNotificationService(mesh, insecure);
const fanportalWishlistQueryClient = new acme.fanportal.v1.WishlistQueryService(mesh, insecure);
const featureflagsFeatureFlagQueryClient = new acme.featureflags.v1.FeatureFlagQueryService(mesh, insecure);
const featureflagsFeatureFlagClient = new acme.featureflags.v1.FeatureFlagService(mesh, insecure);
const featureflagsFeatureFlagValidationClient = new acme.featureflags.v1.FeatureFlagValidationService(mesh, insecure);
const featurestoreFeatureViewClient = new acme.featurestore.v1.FeatureViewService(mesh, insecure);
const fleetVehicleClient = new acme.fleet.v1.VehicleService(mesh, insecure);
const forecastingDemandForecastQueryClient = new acme.forecasting.v1.DemandForecastQueryService(mesh, insecure);
const formsFormCalculationClient = new acme.forms.v1.FormCalculationService(mesh, insecure);
const formsFormClient = new acme.forms.v1.FormService(mesh, insecure);
const fuelFuelCardClient = new acme.fuel.v1.FuelCardService(mesh, insecure);
const fulfillmentFulfillmentQueryClient = new acme.fulfillment.v1.FulfillmentQueryService(mesh, insecure);
const giftcardsGiftCardCalculationClient = new acme.giftcards.v1.GiftCardCalculationService(mesh, insecure);
const helpdeskTicketCalculationClient = new acme.helpdesk.v1.TicketCalculationService(mesh, insecure);
const helpdeskTicketQueryClient = new acme.helpdesk.v1.TicketQueryService(mesh, insecure);
const identityApiCredentialValidationClient = new acme.identity.v1.ApiCredentialValidationService(mesh, insecure);
const identityUserQueryClient = new acme.identity.v1.UserQueryService(mesh, insecure);
const identityUserClient = new acme.identity.v1.UserService(mesh, insecure);
const identityUserValidationClient = new acme.identity.v1.UserValidationService(mesh, insecure);
const invoicingInvoiceBatchCalculationClient = new acme.invoicing.v1.InvoiceBatchCalculationService(mesh, insecure);
const itsmIncidentCalculationClient = new acme.itsm.v1.IncidentCalculationService(mesh, insecure);
const ledgerLedgerAccountQueryClient = new acme.ledger.v1.LedgerAccountQueryService(mesh, insecure);
const ledgerLedgerAccountClient = new acme.ledger.v1.LedgerAccountService(mesh, insecure);
const ledgerLedgerAccountValidationClient = new acme.ledger.v1.LedgerAccountValidationService(mesh, insecure);
const licensingSoftwareLicenseCalculationClient = new acme.licensing.v1.SoftwareLicenseCalculationService(mesh, insecure);
const licensingSoftwareLicenseClient = new acme.licensing.v1.SoftwareLicenseService(mesh, insecure);
const lineageLineageNodeClient = new acme.lineage.v1.LineageNodeService(mesh, insecure);
const loyaltyLoyaltyMemberQueryClient = new acme.loyalty.v1.LoyaltyMemberQueryService(mesh, insecure);
const maintenanceWorkOrderCalculationClient = new acme.maintenance.v1.WorkOrderCalculationService(mesh, insecure);
const maintenanceWorkOrderQueryClient = new acme.maintenance.v1.WorkOrderQueryService(mesh, insecure);
const maintenanceWorkOrderClient = new acme.maintenance.v1.WorkOrderService(mesh, insecure);
const metricsMetricSeriesQueryClient = new acme.metrics.v1.MetricSeriesQueryService(mesh, insecure);
const metricsMetricSeriesClient = new acme.metrics.v1.MetricSeriesService(mesh, insecure);
const metricsMetricSeriesValidationClient = new acme.metrics.v1.MetricSeriesValidationService(mesh, insecure);
const notificationsNotificationQueryClient = new acme.notifications.v1.NotificationQueryService(mesh, insecure);
const notificationsNotificationClient = new acme.notifications.v1.NotificationService(mesh, insecure);
const notificationsNotificationValidationClient = new acme.notifications.v1.NotificationValidationService(mesh, insecure);
const onboardingOnboardingPlanCalculationClient = new acme.onboarding.v1.OnboardingPlanCalculationService(mesh, insecure);
const ordersOrderClient = new acme.orders.v1.OrderService(mesh, insecure);
const patientsPatientQueryClient = new acme.patients.v1.PatientQueryService(mesh, insecure);
const payrollPayRunQueryClient = new acme.payroll.v1.PayRunQueryService(mesh, insecure);
const performersPerformerApprovalClient = new acme.performers.v1.PerformerApprovalService(mesh, insecure);
const performersPerformerContractApprovalClient = new acme.performers.v1.PerformerContractApprovalService(mesh, insecure);
const performersPerformerContractQueryClient = new acme.performers.v1.PerformerContractQueryService(mesh, insecure);
const performersPerformerContractWorkflowClient = new acme.performers.v1.PerformerContractWorkflowService(mesh, insecure);
const performersPerformerQueryClient = new acme.performers.v1.PerformerQueryService(mesh, insecure);
const performersPerformerClient = new acme.performers.v1.PerformerService(mesh, insecure);
const performersPerformerWorkflowClient = new acme.performers.v1.PerformerWorkflowService(mesh, insecure);
const performersRiderApprovalClient = new acme.performers.v1.RiderApprovalService(mesh, insecure);
const performersRiderValidationClient = new acme.performers.v1.RiderValidationService(mesh, insecure);
const performersRiderWorkflowClient = new acme.performers.v1.RiderWorkflowService(mesh, insecure);
const pipelinesPipelineCalculationClient = new acme.pipelines.v1.PipelineCalculationService(mesh, insecure);
const pipelinesPipelineQueryClient = new acme.pipelines.v1.PipelineQueryService(mesh, insecure);
const pipelinesPipelineClient = new acme.pipelines.v1.PipelineService(mesh, insecure);
const prescriptionsPrescriptionClient = new acme.prescriptions.v1.PrescriptionService(mesh, insecure);
const procurementPurchaseRequisitionClient = new acme.procurement.v1.PurchaseRequisitionService(mesh, insecure);
const promotionsPromotionClient = new acme.promotions.v1.PromotionService(mesh, insecure);
const ratelimitingRateLimitQueryClient = new acme.ratelimiting.v1.RateLimitQueryService(mesh, insecure);
const recruitingJobOpeningClient = new acme.recruiting.v1.JobOpeningService(mesh, insecure);
const riskRiskRegisterClient = new acme.risk.v1.RiskRegisterService(mesh, insecure);
const routingRouteClient = new acme.routing.v1.RouteService(mesh, insecure);
const searchSearchIndexQueryClient = new acme.search.v1.SearchIndexQueryService(mesh, insecure);
const searchSearchIndexClient = new acme.search.v1.SearchIndexService(mesh, insecure);
const searchSearchIndexValidationClient = new acme.search.v1.SearchIndexValidationService(mesh, insecure);
const seatingSeatAssignmentApprovalClient = new acme.seating.v1.SeatAssignmentApprovalService(mesh, insecure);
const seatingSeatAssignmentCalculationClient = new acme.seating.v1.SeatAssignmentCalculationService(mesh, insecure);
const seatingSeatAssignmentLifecycleClient = new acme.seating.v1.SeatAssignmentLifecycleService(mesh, insecure);
const seatingSeatAssignmentReportingClient = new acme.seating.v1.SeatAssignmentReportingService(mesh, insecure);
const seatingSeatAssignmentValidationClient = new acme.seating.v1.SeatAssignmentValidationService(mesh, insecure);
const seatingSeatHoldApprovalClient = new acme.seating.v1.SeatHoldApprovalService(mesh, insecure);
const seatingSeatHoldCalculationClient = new acme.seating.v1.SeatHoldCalculationService(mesh, insecure);
const seatingSeatHoldLifecycleClient = new acme.seating.v1.SeatHoldLifecycleService(mesh, insecure);
const seatingSeatHoldQueryClient = new acme.seating.v1.SeatHoldQueryService(mesh, insecure);
const seatingSeatHoldReportingClient = new acme.seating.v1.SeatHoldReportingService(mesh, insecure);
const seatingSeatHoldValidationClient = new acme.seating.v1.SeatHoldValidationService(mesh, insecure);
const seatingSeatHoldWorkflowClient = new acme.seating.v1.SeatHoldWorkflowService(mesh, insecure);
const seatingSeatMapApprovalClient = new acme.seating.v1.SeatMapApprovalService(mesh, insecure);
const seatingSeatMapCalculationClient = new acme.seating.v1.SeatMapCalculationService(mesh, insecure);
const seatingSeatMapLifecycleClient = new acme.seating.v1.SeatMapLifecycleService(mesh, insecure);
const seatingSeatMapQueryClient = new acme.seating.v1.SeatMapQueryService(mesh, insecure);
const seatingSeatMapClient = new acme.seating.v1.SeatMapService(mesh, insecure);
const seatingSeatMapValidationClient = new acme.seating.v1.SeatMapValidationService(mesh, insecure);
const seatingSeatMapWorkflowClient = new acme.seating.v1.SeatMapWorkflowService(mesh, insecure);
const secretsSecretClient = new acme.secrets.v1.SecretService(mesh, insecure);
const shippingShipmentCalculationClient = new acme.shipping.v1.ShipmentCalculationService(mesh, insecure);
const shippingShipmentQueryClient = new acme.shipping.v1.ShipmentQueryService(mesh, insecure);
const showsLineupApprovalClient = new acme.shows.v1.LineupApprovalService(mesh, insecure);
const showsLineupCalculationClient = new acme.shows.v1.LineupCalculationService(mesh, insecure);
const showsLineupLifecycleClient = new acme.shows.v1.LineupLifecycleService(mesh, insecure);
const showsLineupValidationClient = new acme.shows.v1.LineupValidationService(mesh, insecure);
const showsPerformanceApprovalClient = new acme.shows.v1.PerformanceApprovalService(mesh, insecure);
const showsPerformanceCalculationClient = new acme.shows.v1.PerformanceCalculationService(mesh, insecure);
const showsPerformanceQueryClient = new acme.shows.v1.PerformanceQueryService(mesh, insecure);
const showsPerformanceValidationClient = new acme.shows.v1.PerformanceValidationService(mesh, insecure);
const showsShowApprovalClient = new acme.shows.v1.ShowApprovalService(mesh, insecure);
const showsShowCalculationClient = new acme.shows.v1.ShowCalculationService(mesh, insecure);
const showsShowQueryClient = new acme.shows.v1.ShowQueryService(mesh, insecure);
const showsShowClient = new acme.shows.v1.ShowService(mesh, insecure);
const showsShowValidationClient = new acme.shows.v1.ShowValidationService(mesh, insecure);
const showsShowWorkflowClient = new acme.shows.v1.ShowWorkflowService(mesh, insecure);
const sponsorshipsActivationApprovalClient = new acme.sponsorships.v1.ActivationApprovalService(mesh, insecure);
const sponsorshipsActivationCalculationClient = new acme.sponsorships.v1.ActivationCalculationService(mesh, insecure);
const sponsorshipsActivationLifecycleClient = new acme.sponsorships.v1.ActivationLifecycleService(mesh, insecure);
const sponsorshipsActivationValidationClient = new acme.sponsorships.v1.ActivationValidationService(mesh, insecure);
const sponsorshipsActivationWorkflowClient = new acme.sponsorships.v1.ActivationWorkflowService(mesh, insecure);
const sponsorshipsSponsorApprovalClient = new acme.sponsorships.v1.SponsorApprovalService(mesh, insecure);
const sponsorshipsSponsorCalculationClient = new acme.sponsorships.v1.SponsorCalculationService(mesh, insecure);
const sponsorshipsSponsorQueryClient = new acme.sponsorships.v1.SponsorQueryService(mesh, insecure);
const sponsorshipsSponsorClient = new acme.sponsorships.v1.SponsorService(mesh, insecure);
const sponsorshipsSponsorValidationClient = new acme.sponsorships.v1.SponsorValidationService(mesh, insecure);
const sponsorshipsSponsorWorkflowClient = new acme.sponsorships.v1.SponsorWorkflowService(mesh, insecure);
const sponsorshipsSponsorshipDealApprovalClient = new acme.sponsorships.v1.SponsorshipDealApprovalService(mesh, insecure);
const sponsorshipsSponsorshipDealCalculationClient = new acme.sponsorships.v1.SponsorshipDealCalculationService(mesh, insecure);
const sponsorshipsSponsorshipDealQueryClient = new acme.sponsorships.v1.SponsorshipDealQueryService(mesh, insecure);
const sponsorshipsSponsorshipDealValidationClient = new acme.sponsorships.v1.SponsorshipDealValidationService(mesh, insecure);
const sponsorshipsSponsorshipDealWorkflowClient = new acme.sponsorships.v1.SponsorshipDealWorkflowService(mesh, insecure);
const suppliersSupplierClient = new acme.suppliers.v1.SupplierService(mesh, insecure);
const surveysSurveyQueryClient = new acme.surveys.v1.SurveyQueryService(mesh, insecure);
const telematicsTelemetryReadingQueryClient = new acme.telematics.v1.TelemetryReadingQueryService(mesh, insecure);
const ticketingTicketQueryClient = new acme.ticketing.v1.TicketQueryService(mesh, insecure);
const ticketingTicketClient = new acme.ticketing.v1.TicketService(mesh, insecure);
const ticketingTicketTierApprovalClient = new acme.ticketing.v1.TicketTierApprovalService(mesh, insecure);
const ticketingTicketTierQueryClient = new acme.ticketing.v1.TicketTierQueryService(mesh, insecure);
const ticketingTicketTierWorkflowClient = new acme.ticketing.v1.TicketTierWorkflowService(mesh, insecure);
const ticketingTicketTransferApprovalClient = new acme.ticketing.v1.TicketTransferApprovalService(mesh, insecure);
const ticketingTicketTransferValidationClient = new acme.ticketing.v1.TicketTransferValidationService(mesh, insecure);
const ticketingTicketWorkflowClient = new acme.ticketing.v1.TicketWorkflowService(mesh, insecure);
const timekeepingTimesheetQueryClient = new acme.timekeeping.v1.TimesheetQueryService(mesh, insecure);
const toursTourBudgetApprovalClient = new acme.tours.v1.TourBudgetApprovalService(mesh, insecure);
const toursTourBudgetValidationClient = new acme.tours.v1.TourBudgetValidationService(mesh, insecure);
const toursTourBudgetWorkflowClient = new acme.tours.v1.TourBudgetWorkflowService(mesh, insecure);
const toursTourQueryClient = new acme.tours.v1.TourQueryService(mesh, insecure);
const toursTourClient = new acme.tours.v1.TourService(mesh, insecure);
const toursTourStopApprovalClient = new acme.tours.v1.TourStopApprovalService(mesh, insecure);
const toursTourStopQueryClient = new acme.tours.v1.TourStopQueryService(mesh, insecure);
const toursTourStopWorkflowClient = new acme.tours.v1.TourStopWorkflowService(mesh, insecure);
const toursTourWorkflowClient = new acme.tours.v1.TourWorkflowService(mesh, insecure);
const travelTripCalculationClient = new acme.travel.v1.TripCalculationService(mesh, insecure);
const travelTripClient = new acme.travel.v1.TripService(mesh, insecure);
const treasuryCashPositionClient = new acme.treasury.v1.CashPositionService(mesh, insecure);
const venuesVenueAdminClient = new acme.venues.v1.VenueAdminService(mesh, insecure);
const venuesVenueExportClient = new acme.venues.v1.VenueExportService(mesh, insecure);
const venuesVenueHistoryClient = new acme.venues.v1.VenueHistoryService(mesh, insecure);
const venuesVenueNotificationClient = new acme.venues.v1.VenueNotificationService(mesh, insecure);
const venuesVenuePolicyAdminClient = new acme.venues.v1.VenuePolicyAdminService(mesh, insecure);
const venuesVenuePolicyExportClient = new acme.venues.v1.VenuePolicyExportService(mesh, insecure);
const venuesVenuePolicyHistoryClient = new acme.venues.v1.VenuePolicyHistoryService(mesh, insecure);
const venuesVenuePolicyNotificationClient = new acme.venues.v1.VenuePolicyNotificationService(mesh, insecure);
const venuesVenuePolicySyncClient = new acme.venues.v1.VenuePolicySyncService(mesh, insecure);
const venuesVenuePolicyValidationClient = new acme.venues.v1.VenuePolicyValidationService(mesh, insecure);
const venuesVenueQueryClient = new acme.venues.v1.VenueQueryService(mesh, insecure);
const venuesVenueSectionExportClient = new acme.venues.v1.VenueSectionExportService(mesh, insecure);
const venuesVenueSectionHistoryClient = new acme.venues.v1.VenueSectionHistoryService(mesh, insecure);
const venuesVenueSectionNotificationClient = new acme.venues.v1.VenueSectionNotificationService(mesh, insecure);
const venuesVenueSectionQueryClient = new acme.venues.v1.VenueSectionQueryService(mesh, insecure);
const venuesVenueSectionSyncClient = new acme.venues.v1.VenueSectionSyncService(mesh, insecure);
const venuesVenueSectionValidationClient = new acme.venues.v1.VenueSectionValidationService(mesh, insecure);
const venuesVenueClient = new acme.venues.v1.VenueService(mesh, insecure);
const venuesVenueValidationClient = new acme.venues.v1.VenueValidationService(mesh, insecure);
const warehousingWarehouseQueryClient = new acme.warehousing.v1.WarehouseQueryService(mesh, insecure);
const webhooksWebhookEndpointQueryClient = new acme.webhooks.v1.WebhookEndpointQueryService(mesh, insecure);
const yardsYardQueryClient = new acme.yards.v1.YardQueryService(mesh, insecure);
const yardsYardClient = new acme.yards.v1.YardService(mesh, insecure);

/** The clients by the API's fully qualified name, for code that picks one at run time. */
const registry = new Map([
  ['acme.accounting.v1.ChartOfAccountsQueryService', accountingChartOfAccountsQueryClient],
  ['acme.admissions.v1.AdmissionApprovalService', admissionsAdmissionApprovalClient],
  ['acme.admissions.v1.AdmissionCalculationService', admissionsAdmissionCalculationClient],
  ['acme.admissions.v1.AdmissionLifecycleService', admissionsAdmissionLifecycleClient],
  ['acme.admissions.v1.AdmissionQueryService', admissionsAdmissionQueryClient],
  ['acme.admissions.v1.AdmissionService', admissionsAdmissionClient],
  ['acme.admissions.v1.AdmissionValidationService', admissionsAdmissionValidationClient],
  ['acme.admissions.v1.AdmissionWorkflowService', admissionsAdmissionWorkflowClient],
  ['acme.admissions.v1.GateScanApprovalService', admissionsGateScanApprovalClient],
  ['acme.admissions.v1.GateScanCalculationService', admissionsGateScanCalculationClient],
  ['acme.admissions.v1.GateScanLifecycleService', admissionsGateScanLifecycleClient],
  ['acme.admissions.v1.GateScanQueryService', admissionsGateScanQueryClient],
  ['acme.admissions.v1.GateScanValidationService', admissionsGateScanValidationClient],
  ['acme.admissions.v1.GateScanWorkflowService', admissionsGateScanWorkflowClient],
  ['acme.admissions.v1.WristbandApprovalService', admissionsWristbandApprovalClient],
  ['acme.admissions.v1.WristbandCalculationService', admissionsWristbandCalculationClient],
  ['acme.admissions.v1.WristbandLifecycleService', admissionsWristbandLifecycleClient],
  ['acme.admissions.v1.WristbandReportingService', admissionsWristbandReportingClient],
  ['acme.admissions.v1.WristbandValidationService', admissionsWristbandValidationClient],
  ['acme.admissions.v1.WristbandWorkflowService', admissionsWristbandWorkflowClient],
  ['acme.analytics.v1.AnalyticsEventQueryService', analyticsAnalyticsEventQueryClient],
  ['acme.anomalies.v1.AnomalyQueryService', anomaliesAnomalyQueryClient],
  ['acme.anomalies.v1.AnomalyService', anomaliesAnomalyClient],
  ['acme.assets.v1.FixedAssetCalculationService', assetsFixedAssetCalculationClient],
  ['acme.audit.v1.AuditEventQueryService', auditAuditEventQueryClient],
  ['acme.audit.v1.AuditEventService', auditAuditEventClient],
  ['acme.audit.v1.AuditEventValidationService', auditAuditEventValidationClient],
  ['acme.authorization.v1.AccessPolicyQueryService', authorizationAccessPolicyQueryClient],
  ['acme.authorization.v1.AccessPolicyService', authorizationAccessPolicyClient],
  ['acme.authorization.v1.AccessPolicyValidationService', authorizationAccessPolicyValidationClient],
  ['acme.benefits.v1.BenefitPlanQueryService', benefitsBenefitPlanQueryClient],
  ['acme.billing.v1.BillingAccountQueryService', billingBillingAccountQueryClient],
  ['acme.boxoffice.v1.BoxOfficeSaleApprovalService', boxofficeBoxOfficeSaleApprovalClient],
  ['acme.boxoffice.v1.BoxOfficeSaleCalculationService', boxofficeBoxOfficeSaleCalculationClient],
  ['acme.boxoffice.v1.BoxOfficeSaleQueryService', boxofficeBoxOfficeSaleQueryClient],
  ['acme.boxoffice.v1.BoxOfficeSaleService', boxofficeBoxOfficeSaleClient],
  ['acme.boxoffice.v1.BoxOfficeSaleValidationService', boxofficeBoxOfficeSaleValidationClient],
  ['acme.boxoffice.v1.BoxOfficeSaleWorkflowService', boxofficeBoxOfficeSaleWorkflowClient],
  ['acme.boxoffice.v1.CashDrawerApprovalService', boxofficeCashDrawerApprovalClient],
  ['acme.boxoffice.v1.CashDrawerCalculationService', boxofficeCashDrawerCalculationClient],
  ['acme.boxoffice.v1.CashDrawerLifecycleService', boxofficeCashDrawerLifecycleClient],
  ['acme.boxoffice.v1.CashDrawerQueryService', boxofficeCashDrawerQueryClient],
  ['acme.boxoffice.v1.CashDrawerValidationService', boxofficeCashDrawerValidationClient],
  ['acme.boxoffice.v1.CashDrawerWorkflowService', boxofficeCashDrawerWorkflowClient],
  ['acme.boxoffice.v1.GuestListApprovalService', boxofficeGuestListApprovalClient],
  ['acme.boxoffice.v1.GuestListCalculationService', boxofficeGuestListCalculationClient],
  ['acme.boxoffice.v1.GuestListLifecycleService', boxofficeGuestListLifecycleClient],
  ['acme.boxoffice.v1.GuestListReportingService', boxofficeGuestListReportingClient],
  ['acme.boxoffice.v1.GuestListValidationService', boxofficeGuestListValidationClient],
  ['acme.broadcasts.v1.BroadcastApprovalService', broadcastsBroadcastApprovalClient],
  ['acme.broadcasts.v1.BroadcastQueryService', broadcastsBroadcastQueryClient],
  ['acme.broadcasts.v1.BroadcastService', broadcastsBroadcastClient],
  ['acme.broadcasts.v1.BroadcastValidationService', broadcastsBroadcastValidationClient],
  ['acme.broadcasts.v1.BroadcastWorkflowService', broadcastsBroadcastWorkflowClient],
  ['acme.broadcasts.v1.ReplayClipApprovalService', broadcastsReplayClipApprovalClient],
  ['acme.broadcasts.v1.ReplayClipCalculationService', broadcastsReplayClipCalculationClient],
  ['acme.broadcasts.v1.ReplayClipLifecycleService', broadcastsReplayClipLifecycleClient],
  ['acme.broadcasts.v1.ReplayClipValidationService', broadcastsReplayClipValidationClient],
  ['acme.broadcasts.v1.ReplayClipWorkflowService', broadcastsReplayClipWorkflowClient],
  ['acme.broadcasts.v1.StreamKeyApprovalService', broadcastsStreamKeyApprovalClient],
  ['acme.broadcasts.v1.StreamKeyCalculationService', broadcastsStreamKeyCalculationClient],
  ['acme.broadcasts.v1.StreamKeyQueryService', broadcastsStreamKeyQueryClient],
  ['acme.broadcasts.v1.StreamKeyValidationService', broadcastsStreamKeyValidationClient],
  ['acme.broadcasts.v1.StreamKeyWorkflowService', broadcastsStreamKeyWorkflowClient],
  ['acme.budgeting.v1.BudgetQueryService', budgetingBudgetQueryClient],
  ['acme.campaigns.v1.MarketingCampaignQueryService', campaignsMarketingCampaignQueryClient],
  ['acme.campaigns.v1.MarketingCampaignService', campaignsMarketingCampaignClient],
  ['acme.catalog.v1.ProductService', catalogProductClient],
  ['acme.chat.v1.ChatRoomService', chatChatRoomClient],
  ['acme.collections.v1.CollectionCaseService', collectionsCollectionCaseClient],
  ['acme.concessions.v1.ConcessionOrderApprovalService', concessionsConcessionOrderApprovalClient],
  ['acme.concessions.v1.ConcessionOrderCalculationService', concessionsConcessionOrderCalculationClient],
  ['acme.concessions.v1.ConcessionOrderLifecycleService', concessionsConcessionOrderLifecycleClient],
  ['acme.concessions.v1.ConcessionOrderQueryService', concessionsConcessionOrderQueryClient],
  ['acme.concessions.v1.ConcessionOrderReportingService', concessionsConcessionOrderReportingClient],
  ['acme.concessions.v1.ConcessionOrderValidationService', concessionsConcessionOrderValidationClient],
  ['acme.concessions.v1.ConcessionOrderWorkflowService', concessionsConcessionOrderWorkflowClient],
  ['acme.concessions.v1.ConcessionStandApprovalService', concessionsConcessionStandApprovalClient],
  ['acme.concessions.v1.ConcessionStandCalculationService', concessionsConcessionStandCalculationClient],
  ['acme.concessions.v1.ConcessionStandLifecycleService', concessionsConcessionStandLifecycleClient],
  ['acme.concessions.v1.ConcessionStandQueryService', concessionsConcessionStandQueryClient],
  ['acme.concessions.v1.ConcessionStandService', concessionsConcessionStandClient],
  ['acme.concessions.v1.ConcessionStandValidationService', concessionsConcessionStandValidationClient],
  ['acme.concessions.v1.ConcessionStandWorkflowService', concessionsConcessionStandWorkflowClient],
  ['acme.concessions.v1.MenuItemApprovalService', concessionsMenuItemApprovalClient],
  ['acme.concessions.v1.MenuItemCalculationService', concessionsMenuItemCalculationClient],
  ['acme.concessions.v1.MenuItemLifecycleService', concessionsMenuItemLifecycleClient],
  ['acme.concessions.v1.MenuItemReportingService', concessionsMenuItemReportingClient],
  ['acme.concessions.v1.MenuItemValidationService', concessionsMenuItemValidationClient],
  ['acme.concessions.v1.MenuItemWorkflowService', concessionsMenuItemWorkflowClient],
  ['acme.configuration.v1.ConfigSetQueryService', configurationConfigSetQueryClient],
  ['acme.content.v1.ArticleService', contentArticleClient],
  ['acme.contracts.v1.ContractQueryService', contractsContractQueryClient],
  ['acme.corporateevents.v1.CorporateEventCalculationService', corporateeventsCorporateEventCalculationClient],
  ['acme.corporateevents.v1.CorporateEventQueryService', corporateeventsCorporateEventQueryClient],
  ['acme.customers.v1.CustomerCalculationService', customersCustomerCalculationClient],
  ['acme.devices.v1.ManagedDeviceCalculationService', devicesManagedDeviceCalculationClient],
  ['acme.documents.v1.DocumentQueryService', documentsDocumentQueryClient],
  ['acme.documents.v1.DocumentService', documentsDocumentClient],
  ['acme.documents.v1.DocumentValidationService', documentsDocumentValidationClient],
  ['acme.expenses.v1.ExpenseReportCalculationService', expensesExpenseReportCalculationClient],
  ['acme.expenses.v1.ExpenseReportService', expensesExpenseReportClient],
  ['acme.fanportal.v1.EventPageNotificationService', fanportalEventPageNotificationClient],
  ['acme.fanportal.v1.EventPageQueryService', fanportalEventPageQueryClient],
  ['acme.fanportal.v1.EventPageService', fanportalEventPageClient],
  ['acme.fanportal.v1.FanSessionQueryService', fanportalFanSessionQueryClient],
  ['acme.fanportal.v1.FanSessionService', fanportalFanSessionClient],
  ['acme.fanportal.v1.WishlistNotificationService', fanportalWishlistNotificationClient],
  ['acme.fanportal.v1.WishlistQueryService', fanportalWishlistQueryClient],
  ['acme.featureflags.v1.FeatureFlagQueryService', featureflagsFeatureFlagQueryClient],
  ['acme.featureflags.v1.FeatureFlagService', featureflagsFeatureFlagClient],
  ['acme.featureflags.v1.FeatureFlagValidationService', featureflagsFeatureFlagValidationClient],
  ['acme.featurestore.v1.FeatureViewService', featurestoreFeatureViewClient],
  ['acme.fleet.v1.VehicleService', fleetVehicleClient],
  ['acme.forecasting.v1.DemandForecastQueryService', forecastingDemandForecastQueryClient],
  ['acme.forms.v1.FormCalculationService', formsFormCalculationClient],
  ['acme.forms.v1.FormService', formsFormClient],
  ['acme.fuel.v1.FuelCardService', fuelFuelCardClient],
  ['acme.fulfillment.v1.FulfillmentQueryService', fulfillmentFulfillmentQueryClient],
  ['acme.giftcards.v1.GiftCardCalculationService', giftcardsGiftCardCalculationClient],
  ['acme.helpdesk.v1.TicketCalculationService', helpdeskTicketCalculationClient],
  ['acme.helpdesk.v1.TicketQueryService', helpdeskTicketQueryClient],
  ['acme.identity.v1.ApiCredentialValidationService', identityApiCredentialValidationClient],
  ['acme.identity.v1.UserQueryService', identityUserQueryClient],
  ['acme.identity.v1.UserService', identityUserClient],
  ['acme.identity.v1.UserValidationService', identityUserValidationClient],
  ['acme.invoicing.v1.InvoiceBatchCalculationService', invoicingInvoiceBatchCalculationClient],
  ['acme.itsm.v1.IncidentCalculationService', itsmIncidentCalculationClient],
  ['acme.ledger.v1.LedgerAccountQueryService', ledgerLedgerAccountQueryClient],
  ['acme.ledger.v1.LedgerAccountService', ledgerLedgerAccountClient],
  ['acme.ledger.v1.LedgerAccountValidationService', ledgerLedgerAccountValidationClient],
  ['acme.licensing.v1.SoftwareLicenseCalculationService', licensingSoftwareLicenseCalculationClient],
  ['acme.licensing.v1.SoftwareLicenseService', licensingSoftwareLicenseClient],
  ['acme.lineage.v1.LineageNodeService', lineageLineageNodeClient],
  ['acme.loyalty.v1.LoyaltyMemberQueryService', loyaltyLoyaltyMemberQueryClient],
  ['acme.maintenance.v1.WorkOrderCalculationService', maintenanceWorkOrderCalculationClient],
  ['acme.maintenance.v1.WorkOrderQueryService', maintenanceWorkOrderQueryClient],
  ['acme.maintenance.v1.WorkOrderService', maintenanceWorkOrderClient],
  ['acme.metrics.v1.MetricSeriesQueryService', metricsMetricSeriesQueryClient],
  ['acme.metrics.v1.MetricSeriesService', metricsMetricSeriesClient],
  ['acme.metrics.v1.MetricSeriesValidationService', metricsMetricSeriesValidationClient],
  ['acme.notifications.v1.NotificationQueryService', notificationsNotificationQueryClient],
  ['acme.notifications.v1.NotificationService', notificationsNotificationClient],
  ['acme.notifications.v1.NotificationValidationService', notificationsNotificationValidationClient],
  ['acme.onboarding.v1.OnboardingPlanCalculationService', onboardingOnboardingPlanCalculationClient],
  ['acme.orders.v1.OrderService', ordersOrderClient],
  ['acme.patients.v1.PatientQueryService', patientsPatientQueryClient],
  ['acme.payroll.v1.PayRunQueryService', payrollPayRunQueryClient],
  ['acme.performers.v1.PerformerApprovalService', performersPerformerApprovalClient],
  ['acme.performers.v1.PerformerContractApprovalService', performersPerformerContractApprovalClient],
  ['acme.performers.v1.PerformerContractQueryService', performersPerformerContractQueryClient],
  ['acme.performers.v1.PerformerContractWorkflowService', performersPerformerContractWorkflowClient],
  ['acme.performers.v1.PerformerQueryService', performersPerformerQueryClient],
  ['acme.performers.v1.PerformerService', performersPerformerClient],
  ['acme.performers.v1.PerformerWorkflowService', performersPerformerWorkflowClient],
  ['acme.performers.v1.RiderApprovalService', performersRiderApprovalClient],
  ['acme.performers.v1.RiderValidationService', performersRiderValidationClient],
  ['acme.performers.v1.RiderWorkflowService', performersRiderWorkflowClient],
  ['acme.pipelines.v1.PipelineCalculationService', pipelinesPipelineCalculationClient],
  ['acme.pipelines.v1.PipelineQueryService', pipelinesPipelineQueryClient],
  ['acme.pipelines.v1.PipelineService', pipelinesPipelineClient],
  ['acme.prescriptions.v1.PrescriptionService', prescriptionsPrescriptionClient],
  ['acme.procurement.v1.PurchaseRequisitionService', procurementPurchaseRequisitionClient],
  ['acme.promotions.v1.PromotionService', promotionsPromotionClient],
  ['acme.ratelimiting.v1.RateLimitQueryService', ratelimitingRateLimitQueryClient],
  ['acme.recruiting.v1.JobOpeningService', recruitingJobOpeningClient],
  ['acme.risk.v1.RiskRegisterService', riskRiskRegisterClient],
  ['acme.routing.v1.RouteService', routingRouteClient],
  ['acme.search.v1.SearchIndexQueryService', searchSearchIndexQueryClient],
  ['acme.search.v1.SearchIndexService', searchSearchIndexClient],
  ['acme.search.v1.SearchIndexValidationService', searchSearchIndexValidationClient],
  ['acme.seating.v1.SeatAssignmentApprovalService', seatingSeatAssignmentApprovalClient],
  ['acme.seating.v1.SeatAssignmentCalculationService', seatingSeatAssignmentCalculationClient],
  ['acme.seating.v1.SeatAssignmentLifecycleService', seatingSeatAssignmentLifecycleClient],
  ['acme.seating.v1.SeatAssignmentReportingService', seatingSeatAssignmentReportingClient],
  ['acme.seating.v1.SeatAssignmentValidationService', seatingSeatAssignmentValidationClient],
  ['acme.seating.v1.SeatHoldApprovalService', seatingSeatHoldApprovalClient],
  ['acme.seating.v1.SeatHoldCalculationService', seatingSeatHoldCalculationClient],
  ['acme.seating.v1.SeatHoldLifecycleService', seatingSeatHoldLifecycleClient],
  ['acme.seating.v1.SeatHoldQueryService', seatingSeatHoldQueryClient],
  ['acme.seating.v1.SeatHoldReportingService', seatingSeatHoldReportingClient],
  ['acme.seating.v1.SeatHoldValidationService', seatingSeatHoldValidationClient],
  ['acme.seating.v1.SeatHoldWorkflowService', seatingSeatHoldWorkflowClient],
  ['acme.seating.v1.SeatMapApprovalService', seatingSeatMapApprovalClient],
  ['acme.seating.v1.SeatMapCalculationService', seatingSeatMapCalculationClient],
  ['acme.seating.v1.SeatMapLifecycleService', seatingSeatMapLifecycleClient],
  ['acme.seating.v1.SeatMapQueryService', seatingSeatMapQueryClient],
  ['acme.seating.v1.SeatMapService', seatingSeatMapClient],
  ['acme.seating.v1.SeatMapValidationService', seatingSeatMapValidationClient],
  ['acme.seating.v1.SeatMapWorkflowService', seatingSeatMapWorkflowClient],
  ['acme.secrets.v1.SecretService', secretsSecretClient],
  ['acme.shipping.v1.ShipmentCalculationService', shippingShipmentCalculationClient],
  ['acme.shipping.v1.ShipmentQueryService', shippingShipmentQueryClient],
  ['acme.shows.v1.LineupApprovalService', showsLineupApprovalClient],
  ['acme.shows.v1.LineupCalculationService', showsLineupCalculationClient],
  ['acme.shows.v1.LineupLifecycleService', showsLineupLifecycleClient],
  ['acme.shows.v1.LineupValidationService', showsLineupValidationClient],
  ['acme.shows.v1.PerformanceApprovalService', showsPerformanceApprovalClient],
  ['acme.shows.v1.PerformanceCalculationService', showsPerformanceCalculationClient],
  ['acme.shows.v1.PerformanceQueryService', showsPerformanceQueryClient],
  ['acme.shows.v1.PerformanceValidationService', showsPerformanceValidationClient],
  ['acme.shows.v1.ShowApprovalService', showsShowApprovalClient],
  ['acme.shows.v1.ShowCalculationService', showsShowCalculationClient],
  ['acme.shows.v1.ShowQueryService', showsShowQueryClient],
  ['acme.shows.v1.ShowService', showsShowClient],
  ['acme.shows.v1.ShowValidationService', showsShowValidationClient],
  ['acme.shows.v1.ShowWorkflowService', showsShowWorkflowClient],
  ['acme.sponsorships.v1.ActivationApprovalService', sponsorshipsActivationApprovalClient],
  ['acme.sponsorships.v1.ActivationCalculationService', sponsorshipsActivationCalculationClient],
  ['acme.sponsorships.v1.ActivationLifecycleService', sponsorshipsActivationLifecycleClient],
  ['acme.sponsorships.v1.ActivationValidationService', sponsorshipsActivationValidationClient],
  ['acme.sponsorships.v1.ActivationWorkflowService', sponsorshipsActivationWorkflowClient],
  ['acme.sponsorships.v1.SponsorApprovalService', sponsorshipsSponsorApprovalClient],
  ['acme.sponsorships.v1.SponsorCalculationService', sponsorshipsSponsorCalculationClient],
  ['acme.sponsorships.v1.SponsorQueryService', sponsorshipsSponsorQueryClient],
  ['acme.sponsorships.v1.SponsorService', sponsorshipsSponsorClient],
  ['acme.sponsorships.v1.SponsorValidationService', sponsorshipsSponsorValidationClient],
  ['acme.sponsorships.v1.SponsorWorkflowService', sponsorshipsSponsorWorkflowClient],
  ['acme.sponsorships.v1.SponsorshipDealApprovalService', sponsorshipsSponsorshipDealApprovalClient],
  ['acme.sponsorships.v1.SponsorshipDealCalculationService', sponsorshipsSponsorshipDealCalculationClient],
  ['acme.sponsorships.v1.SponsorshipDealQueryService', sponsorshipsSponsorshipDealQueryClient],
  ['acme.sponsorships.v1.SponsorshipDealValidationService', sponsorshipsSponsorshipDealValidationClient],
  ['acme.sponsorships.v1.SponsorshipDealWorkflowService', sponsorshipsSponsorshipDealWorkflowClient],
  ['acme.suppliers.v1.SupplierService', suppliersSupplierClient],
  ['acme.surveys.v1.SurveyQueryService', surveysSurveyQueryClient],
  ['acme.telematics.v1.TelemetryReadingQueryService', telematicsTelemetryReadingQueryClient],
  ['acme.ticketing.v1.TicketQueryService', ticketingTicketQueryClient],
  ['acme.ticketing.v1.TicketService', ticketingTicketClient],
  ['acme.ticketing.v1.TicketTierApprovalService', ticketingTicketTierApprovalClient],
  ['acme.ticketing.v1.TicketTierQueryService', ticketingTicketTierQueryClient],
  ['acme.ticketing.v1.TicketTierWorkflowService', ticketingTicketTierWorkflowClient],
  ['acme.ticketing.v1.TicketTransferApprovalService', ticketingTicketTransferApprovalClient],
  ['acme.ticketing.v1.TicketTransferValidationService', ticketingTicketTransferValidationClient],
  ['acme.ticketing.v1.TicketWorkflowService', ticketingTicketWorkflowClient],
  ['acme.timekeeping.v1.TimesheetQueryService', timekeepingTimesheetQueryClient],
  ['acme.tours.v1.TourBudgetApprovalService', toursTourBudgetApprovalClient],
  ['acme.tours.v1.TourBudgetValidationService', toursTourBudgetValidationClient],
  ['acme.tours.v1.TourBudgetWorkflowService', toursTourBudgetWorkflowClient],
  ['acme.tours.v1.TourQueryService', toursTourQueryClient],
  ['acme.tours.v1.TourService', toursTourClient],
  ['acme.tours.v1.TourStopApprovalService', toursTourStopApprovalClient],
  ['acme.tours.v1.TourStopQueryService', toursTourStopQueryClient],
  ['acme.tours.v1.TourStopWorkflowService', toursTourStopWorkflowClient],
  ['acme.tours.v1.TourWorkflowService', toursTourWorkflowClient],
  ['acme.travel.v1.TripCalculationService', travelTripCalculationClient],
  ['acme.travel.v1.TripService', travelTripClient],
  ['acme.treasury.v1.CashPositionService', treasuryCashPositionClient],
  ['acme.venues.v1.VenueAdminService', venuesVenueAdminClient],
  ['acme.venues.v1.VenueExportService', venuesVenueExportClient],
  ['acme.venues.v1.VenueHistoryService', venuesVenueHistoryClient],
  ['acme.venues.v1.VenueNotificationService', venuesVenueNotificationClient],
  ['acme.venues.v1.VenuePolicyAdminService', venuesVenuePolicyAdminClient],
  ['acme.venues.v1.VenuePolicyExportService', venuesVenuePolicyExportClient],
  ['acme.venues.v1.VenuePolicyHistoryService', venuesVenuePolicyHistoryClient],
  ['acme.venues.v1.VenuePolicyNotificationService', venuesVenuePolicyNotificationClient],
  ['acme.venues.v1.VenuePolicySyncService', venuesVenuePolicySyncClient],
  ['acme.venues.v1.VenuePolicyValidationService', venuesVenuePolicyValidationClient],
  ['acme.venues.v1.VenueQueryService', venuesVenueQueryClient],
  ['acme.venues.v1.VenueSectionExportService', venuesVenueSectionExportClient],
  ['acme.venues.v1.VenueSectionHistoryService', venuesVenueSectionHistoryClient],
  ['acme.venues.v1.VenueSectionNotificationService', venuesVenueSectionNotificationClient],
  ['acme.venues.v1.VenueSectionQueryService', venuesVenueSectionQueryClient],
  ['acme.venues.v1.VenueSectionSyncService', venuesVenueSectionSyncClient],
  ['acme.venues.v1.VenueSectionValidationService', venuesVenueSectionValidationClient],
  ['acme.venues.v1.VenueService', venuesVenueClient],
  ['acme.venues.v1.VenueValidationService', venuesVenueValidationClient],
  ['acme.warehousing.v1.WarehouseQueryService', warehousingWarehouseQueryClient],
  ['acme.webhooks.v1.WebhookEndpointQueryService', webhooksWebhookEndpointQueryClient],
  ['acme.yards.v1.YardQueryService', yardsYardQueryClient],
  ['acme.yards.v1.YardService', yardsYardClient],
]);

function close() {
  for (const client of registry.values()) client.close();
}

module.exports = {
  accountingChartOfAccountsQueryClient,
  admissionsAdmissionApprovalClient,
  admissionsAdmissionCalculationClient,
  admissionsAdmissionLifecycleClient,
  admissionsAdmissionQueryClient,
  admissionsAdmissionClient,
  admissionsAdmissionValidationClient,
  admissionsAdmissionWorkflowClient,
  admissionsGateScanApprovalClient,
  admissionsGateScanCalculationClient,
  admissionsGateScanLifecycleClient,
  admissionsGateScanQueryClient,
  admissionsGateScanValidationClient,
  admissionsGateScanWorkflowClient,
  admissionsWristbandApprovalClient,
  admissionsWristbandCalculationClient,
  admissionsWristbandLifecycleClient,
  admissionsWristbandReportingClient,
  admissionsWristbandValidationClient,
  admissionsWristbandWorkflowClient,
  analyticsAnalyticsEventQueryClient,
  anomaliesAnomalyQueryClient,
  anomaliesAnomalyClient,
  assetsFixedAssetCalculationClient,
  auditAuditEventQueryClient,
  auditAuditEventClient,
  auditAuditEventValidationClient,
  authorizationAccessPolicyQueryClient,
  authorizationAccessPolicyClient,
  authorizationAccessPolicyValidationClient,
  benefitsBenefitPlanQueryClient,
  billingBillingAccountQueryClient,
  boxofficeBoxOfficeSaleApprovalClient,
  boxofficeBoxOfficeSaleCalculationClient,
  boxofficeBoxOfficeSaleQueryClient,
  boxofficeBoxOfficeSaleClient,
  boxofficeBoxOfficeSaleValidationClient,
  boxofficeBoxOfficeSaleWorkflowClient,
  boxofficeCashDrawerApprovalClient,
  boxofficeCashDrawerCalculationClient,
  boxofficeCashDrawerLifecycleClient,
  boxofficeCashDrawerQueryClient,
  boxofficeCashDrawerValidationClient,
  boxofficeCashDrawerWorkflowClient,
  boxofficeGuestListApprovalClient,
  boxofficeGuestListCalculationClient,
  boxofficeGuestListLifecycleClient,
  boxofficeGuestListReportingClient,
  boxofficeGuestListValidationClient,
  broadcastsBroadcastApprovalClient,
  broadcastsBroadcastQueryClient,
  broadcastsBroadcastClient,
  broadcastsBroadcastValidationClient,
  broadcastsBroadcastWorkflowClient,
  broadcastsReplayClipApprovalClient,
  broadcastsReplayClipCalculationClient,
  broadcastsReplayClipLifecycleClient,
  broadcastsReplayClipValidationClient,
  broadcastsReplayClipWorkflowClient,
  broadcastsStreamKeyApprovalClient,
  broadcastsStreamKeyCalculationClient,
  broadcastsStreamKeyQueryClient,
  broadcastsStreamKeyValidationClient,
  broadcastsStreamKeyWorkflowClient,
  budgetingBudgetQueryClient,
  campaignsMarketingCampaignQueryClient,
  campaignsMarketingCampaignClient,
  catalogProductClient,
  chatChatRoomClient,
  collectionsCollectionCaseClient,
  concessionsConcessionOrderApprovalClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionOrderLifecycleClient,
  concessionsConcessionOrderQueryClient,
  concessionsConcessionOrderReportingClient,
  concessionsConcessionOrderValidationClient,
  concessionsConcessionOrderWorkflowClient,
  concessionsConcessionStandApprovalClient,
  concessionsConcessionStandCalculationClient,
  concessionsConcessionStandLifecycleClient,
  concessionsConcessionStandQueryClient,
  concessionsConcessionStandClient,
  concessionsConcessionStandValidationClient,
  concessionsConcessionStandWorkflowClient,
  concessionsMenuItemApprovalClient,
  concessionsMenuItemCalculationClient,
  concessionsMenuItemLifecycleClient,
  concessionsMenuItemReportingClient,
  concessionsMenuItemValidationClient,
  concessionsMenuItemWorkflowClient,
  configurationConfigSetQueryClient,
  contentArticleClient,
  contractsContractQueryClient,
  corporateeventsCorporateEventCalculationClient,
  corporateeventsCorporateEventQueryClient,
  customersCustomerCalculationClient,
  devicesManagedDeviceCalculationClient,
  documentsDocumentQueryClient,
  documentsDocumentClient,
  documentsDocumentValidationClient,
  expensesExpenseReportCalculationClient,
  expensesExpenseReportClient,
  fanportalEventPageNotificationClient,
  fanportalEventPageQueryClient,
  fanportalEventPageClient,
  fanportalFanSessionQueryClient,
  fanportalFanSessionClient,
  fanportalWishlistNotificationClient,
  fanportalWishlistQueryClient,
  featureflagsFeatureFlagQueryClient,
  featureflagsFeatureFlagClient,
  featureflagsFeatureFlagValidationClient,
  featurestoreFeatureViewClient,
  fleetVehicleClient,
  forecastingDemandForecastQueryClient,
  formsFormCalculationClient,
  formsFormClient,
  fuelFuelCardClient,
  fulfillmentFulfillmentQueryClient,
  giftcardsGiftCardCalculationClient,
  helpdeskTicketCalculationClient,
  helpdeskTicketQueryClient,
  identityApiCredentialValidationClient,
  identityUserQueryClient,
  identityUserClient,
  identityUserValidationClient,
  invoicingInvoiceBatchCalculationClient,
  itsmIncidentCalculationClient,
  ledgerLedgerAccountQueryClient,
  ledgerLedgerAccountClient,
  ledgerLedgerAccountValidationClient,
  licensingSoftwareLicenseCalculationClient,
  licensingSoftwareLicenseClient,
  lineageLineageNodeClient,
  loyaltyLoyaltyMemberQueryClient,
  maintenanceWorkOrderCalculationClient,
  maintenanceWorkOrderQueryClient,
  maintenanceWorkOrderClient,
  metricsMetricSeriesQueryClient,
  metricsMetricSeriesClient,
  metricsMetricSeriesValidationClient,
  notificationsNotificationQueryClient,
  notificationsNotificationClient,
  notificationsNotificationValidationClient,
  onboardingOnboardingPlanCalculationClient,
  ordersOrderClient,
  patientsPatientQueryClient,
  payrollPayRunQueryClient,
  performersPerformerApprovalClient,
  performersPerformerContractApprovalClient,
  performersPerformerContractQueryClient,
  performersPerformerContractWorkflowClient,
  performersPerformerQueryClient,
  performersPerformerClient,
  performersPerformerWorkflowClient,
  performersRiderApprovalClient,
  performersRiderValidationClient,
  performersRiderWorkflowClient,
  pipelinesPipelineCalculationClient,
  pipelinesPipelineQueryClient,
  pipelinesPipelineClient,
  prescriptionsPrescriptionClient,
  procurementPurchaseRequisitionClient,
  promotionsPromotionClient,
  ratelimitingRateLimitQueryClient,
  recruitingJobOpeningClient,
  riskRiskRegisterClient,
  routingRouteClient,
  searchSearchIndexQueryClient,
  searchSearchIndexClient,
  searchSearchIndexValidationClient,
  seatingSeatAssignmentApprovalClient,
  seatingSeatAssignmentCalculationClient,
  seatingSeatAssignmentLifecycleClient,
  seatingSeatAssignmentReportingClient,
  seatingSeatAssignmentValidationClient,
  seatingSeatHoldApprovalClient,
  seatingSeatHoldCalculationClient,
  seatingSeatHoldLifecycleClient,
  seatingSeatHoldQueryClient,
  seatingSeatHoldReportingClient,
  seatingSeatHoldValidationClient,
  seatingSeatHoldWorkflowClient,
  seatingSeatMapApprovalClient,
  seatingSeatMapCalculationClient,
  seatingSeatMapLifecycleClient,
  seatingSeatMapQueryClient,
  seatingSeatMapClient,
  seatingSeatMapValidationClient,
  seatingSeatMapWorkflowClient,
  secretsSecretClient,
  shippingShipmentCalculationClient,
  shippingShipmentQueryClient,
  showsLineupApprovalClient,
  showsLineupCalculationClient,
  showsLineupLifecycleClient,
  showsLineupValidationClient,
  showsPerformanceApprovalClient,
  showsPerformanceCalculationClient,
  showsPerformanceQueryClient,
  showsPerformanceValidationClient,
  showsShowApprovalClient,
  showsShowCalculationClient,
  showsShowQueryClient,
  showsShowClient,
  showsShowValidationClient,
  showsShowWorkflowClient,
  sponsorshipsActivationApprovalClient,
  sponsorshipsActivationCalculationClient,
  sponsorshipsActivationLifecycleClient,
  sponsorshipsActivationValidationClient,
  sponsorshipsActivationWorkflowClient,
  sponsorshipsSponsorApprovalClient,
  sponsorshipsSponsorCalculationClient,
  sponsorshipsSponsorQueryClient,
  sponsorshipsSponsorClient,
  sponsorshipsSponsorValidationClient,
  sponsorshipsSponsorWorkflowClient,
  sponsorshipsSponsorshipDealApprovalClient,
  sponsorshipsSponsorshipDealCalculationClient,
  sponsorshipsSponsorshipDealQueryClient,
  sponsorshipsSponsorshipDealValidationClient,
  sponsorshipsSponsorshipDealWorkflowClient,
  suppliersSupplierClient,
  surveysSurveyQueryClient,
  telematicsTelemetryReadingQueryClient,
  ticketingTicketQueryClient,
  ticketingTicketClient,
  ticketingTicketTierApprovalClient,
  ticketingTicketTierQueryClient,
  ticketingTicketTierWorkflowClient,
  ticketingTicketTransferApprovalClient,
  ticketingTicketTransferValidationClient,
  ticketingTicketWorkflowClient,
  timekeepingTimesheetQueryClient,
  toursTourBudgetApprovalClient,
  toursTourBudgetValidationClient,
  toursTourBudgetWorkflowClient,
  toursTourQueryClient,
  toursTourClient,
  toursTourStopApprovalClient,
  toursTourStopQueryClient,
  toursTourStopWorkflowClient,
  toursTourWorkflowClient,
  travelTripCalculationClient,
  travelTripClient,
  treasuryCashPositionClient,
  venuesVenueAdminClient,
  venuesVenueExportClient,
  venuesVenueHistoryClient,
  venuesVenueNotificationClient,
  venuesVenuePolicyAdminClient,
  venuesVenuePolicyExportClient,
  venuesVenuePolicyHistoryClient,
  venuesVenuePolicyNotificationClient,
  venuesVenuePolicySyncClient,
  venuesVenuePolicyValidationClient,
  venuesVenueQueryClient,
  venuesVenueSectionExportClient,
  venuesVenueSectionHistoryClient,
  venuesVenueSectionNotificationClient,
  venuesVenueSectionQueryClient,
  venuesVenueSectionSyncClient,
  venuesVenueSectionValidationClient,
  venuesVenueClient,
  venuesVenueValidationClient,
  warehousingWarehouseQueryClient,
  webhooksWebhookEndpointQueryClient,
  yardsYardQueryClient,
  yardsYardClient,
  registry,
  close,
};
