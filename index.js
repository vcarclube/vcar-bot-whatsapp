/**
 * Bot de WhatsApp com múltiplas instâncias para envio de mensagens em massa
 * Utiliza MySQL com Sequelize para gerenciamento de dados
 */

// Dependências
const { Sequelize, DataTypes, where } = require('sequelize');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const winston = require('winston');
const Queue = require('bull');
const Redis = require('ioredis');

require('dotenv').config();

// Configuração do logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
    new winston.transports.Console({ format: winston.format.simple() })
  ]
});

// Configuração do Express
const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

// Configuração do Redis (para filas)
const redisConfig = {
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT,
  password: process.env.REDIS_PASSWORD
};

// Configuração do MySQL com Sequelize
const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    host: process.env.DB_HOST,
    dialect: 'mysql',
    logging: msg => logger.debug(msg)
  }
);

// Definição dos modelos
const WhatsappInstance = sequelize.define('WhatsappInstance', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  phoneNumber: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  status: {
    type: DataTypes.ENUM('connecting', 'connected', 'disconnected', 'blocked', 'cooldown'),
    defaultValue: 'disconnected'
  },
  dailyMessageCount: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  lastUsed: {
    type: DataTypes.DATE,
    defaultValue: DataTypes.NOW
  },
  dailyLimit: {
    type: DataTypes.INTEGER,
    defaultValue: 100
  },
  cooldownUntil: {
    type: DataTypes.DATE,
    allowNull: true
  }
});

const Lead = sequelize.define('Lead', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  phoneNumber: {
    type: DataTypes.STRING,
    allowNull: false
  },
  name: {
    type: DataTypes.STRING,
    allowNull: true
  },
  email: {
    type: DataTypes.STRING,
    allowNull: true
  },
  status: {
    type: DataTypes.ENUM('pending', 'sent', 'failed', 'replied'),
    defaultValue: 'pending'
  },
  lastMessageSent: {
    type: DataTypes.DATE,
    allowNull: true
  },
  customFields: {
    type: DataTypes.JSON,
    allowNull: true
  }
});

const MessageTemplate = sequelize.define('MessageTemplate', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  content: {
    type: DataTypes.TEXT,
    allowNull: false
  }
});

const MessageLog = sequelize.define('MessageLog', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  leadId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: Lead,
      key: 'id'
    }
  },
  instanceId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: WhatsappInstance,
      key: 'id'
    }
  },
  templateId: {
    type: DataTypes.UUID,
    allowNull: true,
    references: {
      model: MessageTemplate,
      key: 'id'
    }
  },
  status: {
    type: DataTypes.ENUM('queued', 'sent', 'delivered', 'read', 'failed'),
    defaultValue: 'queued'
  },
  errorMessage: {
    type: DataTypes.TEXT,
    allowNull: true
  },
  sentAt: {
    type: DataTypes.DATE,
    allowNull: true
  }
});

const Campaign = sequelize.define('Campaign', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  status: {
    type: DataTypes.ENUM('draft', 'active', 'paused', 'completed'),
    defaultValue: 'draft'
  },
  messageTemplateId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: MessageTemplate,
      key: 'id'
    }
  },
  messageDelay: {
    type: DataTypes.INTEGER,
    defaultValue: 2000 // Delay em milissegundos entre mensagens
  },
  maxDailyMessages: {
    type: DataTypes.INTEGER,
    defaultValue: 500
  }
});

const CampaignLead = sequelize.define('CampaignLead', {
    id: {
      type: DataTypes.UUID,
      defaultValue: DataTypes.UUIDV4,
      primaryKey: true,
      allowNull: false,
    },
    idCampaign: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        allowNull: false,
    },
    idLead: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        allowNull: false,
    },
});
  
Campaign.belongsToMany(Lead, { through: 'CampaignLeads' });
Lead.belongsToMany(Campaign, { through: 'CampaignLeads' });
MessageTemplate.hasMany(Campaign);
Campaign.belongsTo(MessageTemplate);

// Sincronizando os modelos com o banco de dados
const initDatabase = async () => {
  try {
    //await sequelize.sync();
    logger.info('Banco de dados sincronizado com sucesso');
  } catch (error) {
    logger.error('Erro ao sincronizar banco de dados:', error);
    process.exit(1);
  }
};

// Mapa de instâncias ativas de WhatsApp
const activeInstances = new Map();

// Configuração da fila de mensagens usando Bull
const messageQueue = new Queue('whatsapp-messages', {
  redis: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000
    },
    removeOnComplete: true
  }
});

// Gerenciador de instâncias do WhatsApp
class WhatsAppManager {
  constructor() {
    this.instances = new Map();
    this.instanceReadyPromises = new Map();
  }

  // Inicializa todas as instâncias ativas do banco de dados
  async initializeAllInstances() {
    try {
      const instances = await WhatsappInstance.findAll({
        where: { status: { [Sequelize.Op.not]: 'blocked' } }
      });

      for (const instance of instances) {
        await this.initializeInstance(instance.id);
      }
      
      logger.info(`Inicializadas ${instances.length} instâncias de WhatsApp`);
    } catch (error) {
      logger.error('Erro ao inicializar instâncias de WhatsApp:', error);
    }
  }

  // Inicializa uma única instância
  async initializeInstance(instanceId) {
    try {
        const instance = await WhatsappInstance.findByPk(instanceId);
    
        if (!instance) {
          throw new Error(`Instância ${instanceId} não encontrada`);
        }
        
        if (this.instances.has(instanceId)) {
          logger.info(`Instância ${instanceId} já está inicializada`);
          return this.instanceReadyPromises.get(instanceId);
        }

        const baseSessionDir = path.join(__dirname, 'sessions');
        if (!fs.existsSync(baseSessionDir)) {
          fs.mkdirSync(baseSessionDir+"/"+instanceId, { recursive: true });
        }
        
        let resolveReady, rejectReady;
        const readyPromise = new Promise((resolve, reject) => {
          resolveReady = resolve;
          rejectReady = reject;
        });
        this.instanceReadyPromises.set(instanceId, readyPromise);
        
        const client = new Client({
          authStrategy: new LocalAuth({ 
            clientId: instanceId, 
            dataPath: baseSessionDir // Apenas o diretório base
          }),
          puppeteer: {
            headless: true,
            args: [
              '--no-sandbox',
              '--disable-setuid-sandbox',
              '--disable-dev-shm-usage',
              '--disable-accelerated-2d-canvas',
              '--no-first-run',
              '--no-zygote',
              '--single-process',
              '--disable-gpu'
            ]
          }
        });

      client.on('qr', (qr) => {
        // Atualizar status da instância
        instance.update({ status: 'connecting' });

        console.log(qr)
        
        // Gerar QR code no terminal
        qrcode.generate(qr, { small: true });
        logger.info(`QR Code gerado para instância ${instanceId} (${instance.phoneNumber})`);
      });

      client.on('ready', async () => {
        await instance.update({ 
          status: 'connected', 
          lastUsed: new Date(),
          dailyMessageCount: 0 // Reinicia o contador diário
        });
        logger.info(`Instância ${instanceId} (${instance.phoneNumber}) conectada e pronta`);
        resolveReady(client);
      });

      client.on('authenticated', () => {
        logger.info(`Instância ${instanceId} (${instance.phoneNumber}) autenticada`);
      });

      client.on('auth_failure', async (msg) => {
        logger.error(`Falha de autenticação na instância ${instanceId}: ${msg}`);
        await instance.update({ status: 'disconnected' });
        rejectReady(new Error(`Falha de autenticação: ${msg}`));
      });

      client.on('disconnected', async (reason) => {
        logger.warn(`Instância ${instanceId} (${instance.phoneNumber}) desconectada: ${reason}`);
        await instance.update({ status: 'disconnected' });
        
        // Remover a instância da memória
        this.instances.delete(instanceId);
        this.instanceReadyPromises.delete(instanceId);
        
        // Tentar reconectar após alguns segundos
        setTimeout(() => {
          logger.info(`Tentando reconectar instância ${instanceId}...`);
          this.initializeInstance(instanceId);
        }, 30000);
      });

      // Iniciar o cliente
      await client.initialize();
      this.instances.set(instanceId, client);
      return readyPromise;

    } catch (error) {
      logger.error(`Erro ao inicializar instância ${instanceId}:`, error);
      await WhatsappInstance.update(
        { status: 'disconnected' },
        { where: { id: instanceId } }
      );
      throw error;
    }
  }

  // Obter uma instância disponível para envio
  async getAvailableInstance() {
    try {
      // Buscar instâncias disponíveis que não atingiram o limite diário
      const availableInstances = await WhatsappInstance.findAll({
        where: {
          status: 'connected',
          dailyMessageCount: { [Sequelize.Op.lt]: Sequelize.col('dailyLimit') },
          [Sequelize.Op.or]: [
            { cooldownUntil: null },
            { cooldownUntil: { [Sequelize.Op.lt]: new Date() } }
          ]
        },
        order: [['lastUsed', 'ASC']] // Use a instância que está ociosa há mais tempo
      });

      if (availableInstances.length === 0) {
        throw new Error('Nenhuma instância de WhatsApp disponível para envio');
      }

      // Selecionar a primeira instância disponível
      const selectedInstance = availableInstances[0];
      
      // Verificar se a instância já está inicializada
      if (!this.instances.has(selectedInstance.id)) {
        await this.initializeInstance(selectedInstance.id);
      }

      // Esperar até que a instância esteja pronta
      const client = await this.instanceReadyPromises.get(selectedInstance.id);
      
      // Atualizar o timestamp de último uso
      await selectedInstance.update({ lastUsed: new Date() });
      
      return { client, instanceData: selectedInstance };
    } catch (error) {
      logger.error('Erro ao obter instância disponível:', error);
      throw error;
    }
  }

  // Método para enviar mensagem
  async sendMessage(to, message, instanceId = null) {
    let selectedInstance;
    let client;

    try {
      // Se foi especificada uma instância, use-a
      if (instanceId) {
        const instance = await WhatsappInstance.findByPk(instanceId);
        if (!instance || instance.status !== 'connected') {
          throw new Error(`Instância ${instanceId} não está disponível`);
        }
        selectedInstance = instance;
        client = await this.instanceReadyPromises.get(instanceId);
      } else {
        // Senão, obtenha uma instância disponível
        const result = await this.getAvailableInstance();
        selectedInstance = result.instanceData;
        client = result.client;
      }

      // Formatar o número de telefone 
      const formattedNumber = this.formatPhoneNumber(to);
      
      // Verificar se o número está no WhatsApp
      const isRegistered = await client.isRegisteredUser(`${formattedNumber}@c.us`);
      if (!isRegistered) {
        throw new Error(`O número ${to} não está registrado no WhatsApp`);
      }

      // Enviar a mensagem
      const result = await client.sendMessage(`${formattedNumber}@c.us`, message);

      // Incrementar o contador de mensagens diárias
      await selectedInstance.increment('dailyMessageCount');
      
      // Verificar se a instância atingiu o limite diário
      if (selectedInstance.dailyMessageCount >= selectedInstance.dailyLimit) {
        // Colocar em cooldown por 24h
        const cooldownTime = new Date();
        cooldownTime.setHours(cooldownTime.getHours() + 24);
        
        await selectedInstance.update({
          status: 'cooldown',
          cooldownUntil: cooldownTime
        });
        
        logger.info(`Instância ${selectedInstance.id} colocada em cooldown por 24h`);
      }
      
      return { success: true, messageId: result.id._serialized, instanceId: selectedInstance.id };
    } catch (error) {
      logger.error(`Erro ao enviar mensagem para ${to}:`, error);
      
      // Se ocorrer um erro que sugere bloqueio, marcar a instância como bloqueada
      if (error.message.includes('blocked') || error.message.includes('spam')) {
        await selectedInstance.update({ status: 'blocked' });
        logger.warn(`Instância ${selectedInstance.id} marcada como bloqueada`);
      }
      
      throw error;
    }
  }

  // Formatar número de telefone para o formato esperado pelo WhatsApp
  formatPhoneNumber(phoneNumber) {
    // Remover caracteres não numéricos
    let cleaned = phoneNumber.replace(/\D/g, '');
    
    // Verificar se já tem o código do país
    if (!cleaned.startsWith('55')) {
      cleaned = '55' + cleaned;
    }
    
    return cleaned;
  }

  // Desconectar todas as instâncias
  async disconnectAll() {
    for (const [instanceId, client] of this.instances.entries()) {
      try {
        await client.destroy();
        logger.info(`Instância ${instanceId} desconectada com sucesso`);
      } catch (error) {
        logger.error(`Erro ao desconectar instância ${instanceId}:`, error);
      }
    }
    
    this.instances.clear();
    this.instanceReadyPromises.clear();
  }
}

// Inicializar o gerenciador de WhatsApp
const whatsappManager = new WhatsAppManager();

// Processar jobs da fila de mensagens
messageQueue.process(async (job) => {
  const { leadId, campaignId, templateId } = job.data;
  
  try {
    // Buscar os dados necessários
    const lead = await Lead.findByPk(leadId);
    const campaign = await Campaign.findByPk(campaignId);
    const template = await MessageTemplate.findByPk(templateId);
    
    if (!lead || !campaign || !template) {
      throw new Error('Dados incompletos para envio de mensagem');
    }
    
    // Personalizar a mensagem com dados do lead
    let messageContent = template.content;
    
    // Substituir variáveis na mensagem
    messageContent = messageContent.replace(/{nome}/g, lead.name || '');
    messageContent = messageContent.replace(/{email}/g, lead.email || '');
    
    // Substituir campos personalizados
    if (lead.customFields) {
      for (const [key, value] of Object.entries(lead.customFields)) {
        messageContent = messageContent.replace(new RegExp(`{${key}}`, 'g'), value || '');
      }
    }
    
    // Enviar a mensagem
    const result = await whatsappManager.sendMessage(lead.phoneNumber, messageContent);
    
    // Registrar o envio
    await MessageLog.create({
      leadId: lead.id,
      instanceId: result.instanceId,
      templateId: template.id,
      status: 'sent',
      sentAt: new Date()
    });
    
    // Atualizar o status do lead
    await lead.update({
      status: 'sent',
      lastMessageSent: new Date()
    });
    
    return { success: true, messageId: result.messageId };
  } catch (error) {
    logger.error(`Erro ao processar job da fila (Lead ID: ${leadId}):`, error);
    
    // Registrar a falha
    await MessageLog.create({
      leadId,
      instanceId: null,
      templateId,
      status: 'failed',
      errorMessage: error.message,
      sentAt: new Date()
    });
    
    // Marcar o lead como falha se já tentou 3 vezes
    if (job.attemptsMade >= 2) {
      await Lead.update(
        { status: 'failed' },
        { where: { id: leadId } }
      );
    }
    
    throw error;
  }
});

// Função para adicionar leads a uma campanha e enfileirar mensagens
async function addLeadsToCampaign(campaignId, leadIds) {
  try {
    const campaign = await Campaign.findByPk(campaignId);
    if (!campaign) {
      throw new Error(`Campanha ${campaignId} não encontrada`);
    }
    
    const leads = await Lead.findAll({
      where: {
        id: leadIds
      }
    });
    
    // Adicionar leads à campanha
    const registros = leadIds.map(leadId => ({
        idCampaign: campaignId,
        idLead: leadId
    }));
    
    await CampaignLead.bulkCreate(registros);
    
    // Enfileirar mensagens para cada lead
    for (let i = 0; i < leads.length; i++) {
      const lead = leads[i];
      
      // Calcular o atraso com base na posição na fila e no delay configurado
      const delay = i * campaign.messageDelay;
      
      // Enfileirar a mensagem com o atraso calculado
      await messageQueue.add({
        leadId: lead.id,
        campaignId: campaign.id,
        templateId: campaign.messageTemplateId
      }, {
        delay: delay,
        jobId: `${campaign.id}-${lead.id}-${Date.now()}`
      });
      
      logger.info(`Mensagem enfileirada para ${lead.phoneNumber} com delay de ${delay}ms`);
    }
    
    return {
      success: true,
      enqueued: leads.length,
      campaignId
    };
  } catch (error) {
    logger.error(`Erro ao adicionar leads à campanha ${campaignId}:`, error);
    throw error;
  }
}

// Rotas da API
app.get('/api/status', (req, res) => {
  res.json({
    status: 'online',
    instances: whatsappManager.instances.size,
    queueSize: messageQueue.getJobCounts()
  });
});

// Rota para criar uma nova instância do WhatsApp
app.post('/api/instances', async (req, res) => {
  try {
    const { phoneNumber, name, dailyLimit } = req.body;
    
    if (!phoneNumber || !name) {
      return res.status(400).json({ error: 'Número de telefone e nome são obrigatórios' });
    }
    
    // Criar a nova instância no banco
    const instance = await WhatsappInstance.create({
      phoneNumber,
      name,
      dailyLimit: dailyLimit || 100,
      status: 'disconnected'
    });
    
    // Inicializar a instância
    whatsappManager.initializeInstance(instance.id)
      .then(() => {
        logger.info(`Instância ${instance.id} inicializada com sucesso`);
      })
      .catch(err => {
        logger.error(`Erro ao inicializar instância ${instance.id}:`, err);
      });
    
    res.status(201).json({
      id: instance.id,
      phoneNumber: instance.phoneNumber,
      name: instance.name,
      status: instance.status,
      message: 'Instância criada. Escaneie o QR code no console para autenticar.'
    });
  } catch (error) {
    logger.error('Erro ao criar instância:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para listar instâncias
app.get('/api/instances', async (req, res) => {
  try {
    const instances = await WhatsappInstance.findAll();
    res.json(instances);
  } catch (error) {
    logger.error('Erro ao listar instâncias:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para criar templates de mensagem
app.post('/api/templates', async (req, res) => {
  try {
    const { name, content } = req.body;
    
    if (!name || !content) {
      return res.status(400).json({ error: 'Nome e conteúdo são obrigatórios' });
    }
    
    const template = await MessageTemplate.create({
      name,
      content
    });
    
    res.status(201).json({
      id: template.id,
      name: template.name,
      content: template.content
    });
  } catch (error) {
    logger.error('Erro ao criar template:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para listar templates
app.get('/api/templates', async (req, res) => {
  try {
    const templates = await MessageTemplate.findAll();
    res.json(templates);
  } catch (error) {
    logger.error('Erro ao listar templates:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para adicionar leads
app.post('/api/leads', async (req, res) => {
  try {
    const { leads } = req.body;

    console.log(leads)
    
    if (!Array.isArray(leads) || leads.length === 0) {
      return res.status(400).json({ error: 'É necessário fornecer uma lista de leads' });
    }
    
    const createdLeads = await Lead.bulkCreate(leads);
    
    res.status(201).json({
      success: true,
      count: createdLeads.length,
      leads: createdLeads
    });
  } catch (error) {
    logger.error('Erro ao adicionar leads:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para criar campanha
app.post('/api/campaigns', async (req, res) => {
  try {
    const { name, templateId, messageDelay, maxDailyMessages } = req.body;
    
    if (!name || !templateId) {
      return res.status(400).json({ error: 'Nome e ID do template são obrigatórios' });
    }
    
    const campaign = await Campaign.create({
      name,
      messageTemplateId: templateId,
      messageDelay: messageDelay || 2000,
      maxDailyMessages: maxDailyMessages || 500,
      status: 'draft'
    });
    
    res.status(201).json({
      id: campaign.id,
      name: campaign.name,
      status: campaign.status
    });
  } catch (error) {
    logger.error('Erro ao criar campanha:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para adicionar leads a uma campanha
app.post('/api/add-lead-to-campaign', async (req, res) => {
  try {
    const { leadIds, campaignId } = req.body;
    
    if (!Array.isArray(leadIds) || leadIds.length === 0) {
      return res.status(400).json({ error: 'É necessário fornecerd uma lista de IDs de leads' });
    }
    
    const result = await addLeadsToCampaign(campaignId, leadIds);
    
    res.json(result);
  } catch (error) {
    logger.error(`Erro ao adicionar leads à campanha ${req.params.campaignId}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para ativar uma campanha
app.post('/api/campaigns/activate', async (req, res) => {
  try {
    const { campaignId } = req.body;

    const campaign = await Campaign.findOne({
        where: { 
            id: campaignId
        }
    });

    if (!campaign) {
      return res.status(404).json({ error: 'Campanha não encontrada' });
    }
    
    await campaign.update({ 
        where: {
            status: 'active'
        }
    });

    const campaignLead = await CampaignLead.findAll({
        where: {
            idCampaign: campaign.id
        }
    })

    const leadIds = campaignLead.map(lead => lead.idLead);

    console.log(campaign, campaignLead, leadIds)

    const result = await addLeadsToCampaign(campaignId, leadIds);
    
    return res.status(200).json({
      campaignId,
      status: 'active',
      leadsEnqueued: result.enqueued
    });
  } catch (error) {
    logger.error(`Erro ao ativar campanha ${req.body}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para obter estatísticas de uma campanha
app.get('/api/campaigns/stats', async (req, res) => {
  try {
    const { campaignId } = req.body;
    
    // Verificar se a campanha existe
    const campaign = await Campaign.findByPk(campaignId);
    if (!campaign) {
      return res.status(404).json({ error: 'Campanha não encontrada' });
    }
    
    // Obter todos os leads desta campanha
    const leads = await campaign.getLeads();
    
    // Obter estatísticas de mensagens
    const messageLogs = await MessageLog.findAll({
      where: {
        leadId: leads.map(lead => lead.id)
      },
      attributes: [
        'status',
        [sequelize.fn('COUNT', sequelize.col('id')), 'count']
      ],
      group: ['status']
    });
    
    // Formatar as estatísticas
    const stats = {
      campaignId,
      campaignName: campaign.name,
      totalLeads: leads.length,
      messages: {
        queued: 0,
        sent: 0,
        delivered: 0,
        read: 0,
        failed: 0
      }
    };
    
    // Preencher as estatísticas
    messageLogs.forEach(log => {
      stats.messages[log.status] = parseInt(log.getDataValue('count'));
    });
    
    res.json(stats);
  } catch (error) {
    logger.error(`Erro ao obter estatísticas da campanha ${req.params.campaignId}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Inicializar o servidor
async function startServer() {
  try {
    // Inicializar o banco de dados
    await initDatabase();
    
    // Inicializar as instâncias do WhatsApp
    await whatsappManager.initializeAllInstances();
    
    // Iniciar o servidor HTTP
    app.listen(PORT, () => {
      logger.info(`Servidor rodando na porta ${PORT}`);
    });
    
    // Configurar encerramento gracioso
    process.on('SIGINT', async () => {
      logger.info('Encerrando servidor...');
      
      // Desconectar todas as instâncias do WhatsApp
      await whatsappManager.disconnectAll();
      
      // Fechar conexão com o banco de dados
      await sequelize.close();
      
      // Fechar o servidor Bull/Redis
      await messageQueue.close();
      
      logger.info('Servidor encerrado com sucesso');
      process.exit(0);
    });
  } catch (error) {
    logger.error('Erro ao iniciar o servidor:', error);
    process.exit(1);
  }
}

// Função para verificar e resetar contadores diários à meia-noite
async function setupDailyReset() {
  // Definir o horário para resetar (meia-noite)
  const now = new Date();
  const night = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate() + 1, // próximo dia
    0, 0, 0 // 00:00:00
  );
  
  // Calcular milissegundos até meia-noite
  const msToMidnight = night.getTime() - now.getTime();
  
  // Configurar o timer
  setTimeout(async () => {
    try {
      // Resetar os contadores de mensagens diárias
      await WhatsappInstance.update(
        { 
          dailyMessageCount: 0,
          status: sequelize.literal(`CASE 
            WHEN status = 'cooldown' AND cooldownUntil <= NOW() THEN 'connected' 
            WHEN status = 'cooldown' THEN status 
            ELSE status END`),
          cooldownUntil: sequelize.literal(`CASE 
            WHEN status = 'cooldown' AND cooldownUntil <= NOW() THEN NULL 
            ELSE cooldownUntil END`)
        },
        { where: {} }
      );
      
      logger.info('Contadores diários de mensagens resetados com sucesso');
      
      // Configurar o próximo reset
      setupDailyReset();
    } catch (error) {
      logger.error('Erro ao resetar contadores diários:', error);
      // Tentar novamente em 1 minuto em caso de falha
      setTimeout(setupDailyReset, 60000);
    }
  }, msToMidnight);
  
  logger.info(`Próximo reset programado para ${night.toLocaleString()}`);
}

// Rota para enviar mensagem direta para um lead
app.post('/api/send-direct', async (req, res) => {
  try {
    const { phoneNumber, message, instanceId } = req.body;
    
    if (!phoneNumber || !message) {
      return res.status(400).json({ error: 'Número de telefone e mensagem são obrigatórios' });
    }
    
    // Enviar mensagem
    const result = await whatsappManager.sendMessage(phoneNumber, message, instanceId);
    
    res.json({
      success: true,
      messageId: result.messageId,
      instanceId: result.instanceId
    });
  } catch (error) {
    logger.error('Erro ao enviar mensagem direta:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para importar leads de um arquivo CSV
app.post('/api/import-leads', express.raw({ type: 'text/csv', limit: '10mb' }), async (req, res) => {
  try {
    const csvData = req.body.toString('utf8');
    const rows = csvData.trim().split('\n');
    const headers = rows[0].split(',').map(header => header.trim());
    
    const leads = [];
    
    // Processar as linhas do CSV (ignorando o cabeçalho)
    for (let i = 1; i < rows.length; i++) {
      const values = rows[i].split(',');
      
      // Verificar se a linha tem o número mínimo de campos
      if (values.length < 2) continue;
      
      const lead = {
        phoneNumber: values[headers.indexOf('phoneNumber')] || values[headers.indexOf('phone')] || values[0],
        name: values[headers.indexOf('name')] || values[1] || null,
        email: values[headers.indexOf('email')] || (values.length > 2 ? values[2] : null) || null,
        status: 'pending',
        customFields: {}
      };
      
      // Adicionar campos personalizados
      for (let j = 0; j < headers.length; j++) {
        const header = headers[j];
        if (!['phoneNumber', 'phone', 'name', 'email'].includes(header)) {
          lead.customFields[header] = values[j] || null;
        }
      }
      
      leads.push(lead);
    }
    
    // Verificar se há leads para importar
    if (leads.length === 0) {
      return res.status(400).json({ error: 'Nenhum lead válido encontrado no CSV' });
    }
    
    // Salvar leads no banco
    const createdLeads = await Lead.bulkCreate(leads);
    
    res.status(201).json({
      success: true,
      imported: createdLeads.length,
      leads: createdLeads.map(lead => ({
        id: lead.id,
        phoneNumber: lead.phoneNumber,
        name: lead.name
      }))
    });
  } catch (error) {
    logger.error('Erro ao importar leads:', error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para pausar/retomar uma campanha
app.post('/api/campaigns/:campaignId/toggle', async (req, res) => {
  try {
    const { campaignId } = req.params;
    
    const campaign = await Campaign.findByPk(campaignId);
    if (!campaign) {
      return res.status(404).json({ error: 'Campanha não encontrada' });
    }
    
    // Alternar entre os estados paused/active
    const newStatus = campaign.status === 'active' ? 'paused' : 'active';
    
    await campaign.update({ status: newStatus });
    
    // Se estiver ativando, reprocessar leads pendentes
    if (newStatus === 'active') {
      // Obter leads pendentes
      const pendingLeads = await Lead.findAll({
        include: [{
          model: Campaign,
          where: { id: campaignId }
        }],
        where: { status: 'pending' }
      });
      
      if (pendingLeads.length > 0) {
        const leadIds = pendingLeads.map(lead => lead.id);
        await addLeadsToCampaign(campaignId, leadIds);
      }
    }
    
    res.json({
      campaignId,
      status: newStatus,
      message: `Campanha ${newStatus === 'active' ? 'ativada' : 'pausada'} com sucesso`
    });
  } catch (error) {
    logger.error(`Erro ao alternar estado da campanha ${req.params.campaignId}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para excluir uma instância
app.delete('/api/instances/:instanceId', async (req, res) => {
  try {
    const { instanceId } = req.params;
    
    // Verificar se a instância existe
    const instance = await WhatsappInstance.findByPk(instanceId);
    if (!instance) {
      return res.status(404).json({ error: 'Instância não encontrada' });
    }
    
    // Desconectar a instância se estiver ativa
    if (whatsappManager.instances.has(instanceId)) {
      const client = whatsappManager.instances.get(instanceId);
      await client.destroy();
      whatsappManager.instances.delete(instanceId);
      whatsappManager.instanceReadyPromises.delete(instanceId);
    }
    
    // Remover a pasta de sessão
    const sessionDir = path.join(__dirname, 'sessions', instanceId);
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }
    
    // Remover do banco de dados
    await instance.destroy();
    
    res.json({
      success: true,
      message: `Instância ${instanceId} removida com sucesso`
    });
  } catch (error) {
    logger.error(`Erro ao excluir instância ${req.params.instanceId}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Rota para visualizar filas de mensagens
app.get('/api/queue/status', async (req, res) => {
  try {
    const counts = await messageQueue.getJobCounts();
    
    res.json({
      stats: counts,
      active: await messageQueue.getActive(),
      waiting: await messageQueue.getWaiting(0, 10), // Mostrar apenas os primeiros 10
      delayed: await messageQueue.getDelayed(0, 10), // Mostrar apenas os primeiros 10
      failed: await messageQueue.getFailed(0, 10)    // Mostrar apenas os primeiros 10
    });
  } catch (error) {
    logger.error('Erro ao obter status da fila:', error);
    res.status(500).json({ error: error.message });
  }
});

// Programar a verificação de novas mensagens recebidas
async function setupMessageListener() {
  try {
    // Para cada instância ativa
    for (const [instanceId, client] of whatsappManager.instances.entries()) {
      // Configurar ouvinte de mensagens
      client.on('message', async (msg) => {
        if (msg.from.endsWith('@c.us')) { // Mensagem de usuário, não de grupo
          // Extrair número de telefone
          const phoneNumber = msg.from.split('@')[0];
          
          // Registrar a mensagem recebida
          logger.info(`Mensagem recebida de ${phoneNumber}: ${msg.body}`);
          
          // Buscar o lead correspondente
          const lead = await Lead.findOne({
            where: {
              phoneNumber: {
                [Sequelize.Op.like]: `%${phoneNumber.slice(-8)}%` // Buscar pelos últimos 8 dígitos
              }
            }
          });
          
          // Se encontrou o lead, marcar como respondido
          if (lead) {
            await lead.update({ status: 'replied' });
            logger.info(`Lead ${lead.id} marcado como "replied"`);
          }
        }
      });
    }
  } catch (error) {
    logger.error('Erro ao configurar ouvinte de mensagens:', error);
  }
}

// Função para verificar saúde das instâncias periodicamente
async function monitorInstancesHealth() {
  setInterval(async () => {
    try {
      // Verificar instâncias conectadas que não estão respondendo
      const connectedInstances = await WhatsappInstance.findAll({
        where: { status: 'connected' }
      });
      
      for (const instance of connectedInstances) {
        // Verificar se a instância está na memória
        if (!whatsappManager.instances.has(instance.id)) {
          logger.warn(`Instância ${instance.id} marcada como conectada, mas não está na memória. Reinicializando...`);
          
          // Tentar inicializar novamente
          try {
            await whatsappManager.initializeInstance(instance.id);
          } catch (initError) {
            logger.error(`Falha ao reinicializar instância ${instance.id}:`, initError);
            
            // Marcar como desconectada se não conseguir inicializar
            await instance.update({ status: 'disconnected' });
          }
        }
      }
      
      // Verificar instâncias em cooldown que já podem ser reativadas
      const cooldownInstances = await WhatsappInstance.findAll({
        where: {
          status: 'cooldown',
          cooldownUntil: { [Sequelize.Op.lt]: new Date() }
        }
      });
      
      for (const instance of cooldownInstances) {
        logger.info(`Reativando instância ${instance.id} após período de cooldown`);
        
        await instance.update({
          status: 'connected',
          cooldownUntil: null,
          dailyMessageCount: 0
        });
      }
    } catch (error) {
      logger.error('Erro ao monitorar saúde das instâncias:', error);
    }
  }, 5 * 60 * 1000); // Verificar a cada 5 minutos
}

// Iniciar o servidor e configurar tarefas periódicas
startServer()
  .then(() => {
    // Configurar reset diário
    setupDailyReset();
    
    // Configurar ouvinte de mensagens
    setTimeout(() => {
      setupMessageListener();
    }, 10000); // Esperar 10 segundos para as instâncias inicializarem
    
    // Iniciar monitoramento de saúde
    monitorInstancesHealth();
  })
  .catch(error => {
    logger.error('Falha ao iniciar o sistema:', error);
    process.exit(1);
  });

// Exportar para uso em testes ou como módulo
module.exports = {
  app,
  whatsappManager,
  sequelize,
  messageQueue
};