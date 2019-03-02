using System;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;

using Framework.Core;
using IBM.WMQ;

namespace Framework.MQ.IBM
{
    /// <summary>
    /// This class is in charge of providing the communication gateway to the IBM MQ Series services
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class MQ : IDisposable
    {
        #region| Fields |

        /// <summary>
        /// Queue name in the IBM MQ
        /// </summary>
        private string QueueName { get; set; }

        /// <summary>
        /// Queue manager name from IBM MQ
        /// </summary>
        private string QueueManagerName { get; set; }

        /// <summary>
        /// Queue manager component
        /// </summary>
        private MQQueueManager QueueManager;

        /// <summary>
        /// Properties related to the IBM MQ connection
        /// </summary>
        private Hashtable Properties;

        #endregion

        #region| Properties |

        public bool HasError { get; set; }
        public string ErrorMessage { get; set; }
        public Exception Exception { get; set; }

        #endregion

        #region| Constructor |

        /// <summary>
        /// Default Constructor
        /// </summary>
        /// <param name="queueManagerName">Queue manager name</param>
        public MQ(string queueManagerName)
        {
            queueManagerName.ThrowIfNull("The queue manager name cannot be null or empty");

            this.QueueManagerName = queueManagerName;
        }

        #endregion

        #region| Methods |

        /// <summary>
        /// Starts a MQ connection
        /// </summary>
        /// <param name="hostName">HostName</param>
        /// <param name="channelName">Channel</param>
        /// <param name="portNumber">Port Number</param>
        /// <param name="UserID">User ID from WebSphere queue</param>
        public void Connect(string hostName, string channelName, string portNumber, string UserID = "")
        {
            this.Properties = new Hashtable();

            Properties.Add(MQC.HOST_NAME_PROPERTY, hostName);
            Properties.Add(MQC.CHANNEL_PROPERTY, channelName);
            Properties.Add(MQC.PORT_PROPERTY, portNumber);

            if (UserID.IsNotNull())
            {
                Properties.Add(MQC.USER_ID_PROPERTY, UserID);
            }

            Connect(Properties);

        }

        /// <summary>
        /// Conects to a MQ providing a property hastable with important information
        /// </summary>
        /// <param name="props">Hashtable of properties</param>
        public void Connect(Hashtable props)
        {
            this.Properties = props;

            CheckProperties();

            try
            {
                QueueManager = new MQQueueManager(QueueManagerName, props);
            }
            catch (Exception oException)
            {
                throw oException;
            }
        }

        /// <summary>
        /// Writes a message into the IBM MQ Server
        /// </summary>
        /// <param name="message">message content</param>
        /// <param name="queueName">Queue name</param>
        public void Write(string message, string queueName, bool KeepAlive = true)
        {
            message.ThrowIfNull("The message parameter cannot be null or empty");

            CheckConection(queueName);

            MQMessage queueMessage;
            MQPutMessageOptions queuePutMessageOptions;
            MQQueue queue;

            try
            {
                queueMessage = new MQMessage();
                queuePutMessageOptions = new MQPutMessageOptions();
                queue = QueueManager.AccessQueue(queueName, MQC.MQOO_OUTPUT + MQC.MQOO_FAIL_IF_QUIESCING);

                queueMessage.Encoding = 437;
                queueMessage.Write(Encoding.UTF8.GetBytes(message));
                queueMessage.Format = MQC.MQFMT_NONE;

                queue.Put(queueMessage, queuePutMessageOptions);
            }
            catch (Exception oException)
            {
                GetErrorDetails(oException);
            }
            finally
            {
                queueMessage = null;
                queuePutMessageOptions = null;
                queue = null;

                if (!KeepAlive)
                {
                    Close();
                }

            }
        }

        /// <summary>
        /// Read the first message in the queue without remove it from the stack
        /// </summary>
        /// <param name="queueName">Queue name</param>
        /// <returns>message content</returns>
        public string ReadOnly(string queueName, bool KeepAlive = true)
        {
            CheckConection(queueName);

            lock (this)
            {
                string msg = string.Empty;

                MQQueue queue;
                MQMessage queueMessage;
                MQGetMessageOptions queueGetMessageOptions;

                try
                {
                    // Read a message without removing it from the queue (ReadOnly)
                    queue = QueueManager.AccessQueue(queueName, MQC.MQOO_INPUT_AS_Q_DEF + MQC.MQOO_FAIL_IF_QUIESCING + MQC.MQOO_INQUIRE + MQC.MQOO_BROWSE);

                    queueMessage = new MQMessage()
                    {
                        Format = MQC.MQFMT_STRING,
                        CharacterSet = 1208
                    };

                    queueGetMessageOptions = new MQGetMessageOptions();

                    if (queue.CurrentDepth > 0)
                    {
                        queueGetMessageOptions.Options = MQC.MQGMO_WAIT | MQC.MQGMO_BROWSE_FIRST;
                        queue.Get(queueMessage, queueGetMessageOptions);

                        if (queueMessage.MessageLength > 0)
                        {
                            msg = queueMessage.ReadString(queueMessage.MessageLength);
                        }
                    }
                }
                catch (Exception oException)
                {
                    GetErrorDetails(oException);

                    return string.Empty;
                }
                finally
                {
                    queue = null;
                    queueMessage = null;
                    queueGetMessageOptions = null;

                    if (!KeepAlive)
                    {
                        Close();
                    }
                }

                return msg;
            }
        }

        /// <summary>
        ///  Read the first message in the queue, appends the message content in the given file and remove the message from the queue
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="FilePath"></param>
        /// <param name="Encoding"></param>
        /// <param name="KeepAlive"></param>
        /// <returns></returns>
        public string ReadSafe(string queueName, string FilePath, Encoding Encoding, bool KeepAlive = true)
        {
            // Reads the message in a read-only way
            var msg = ReadOnly(queueName, KeepAlive);

            try
            {
                if (msg.IsNotNull())
                {
                    // Saves the message in a file 
                    File.AppendAllText(FilePath, msg, Encoding);
                }

                // Removes the message from the queue
                Read(queueName, KeepAlive);
            }
            catch (Exception oException)
            {
                GetErrorDetails(oException);
            }

            return msg;

        }

        /// <summary>
        /// Reads a queue from IBM MQ Series and removes the message from que queue automatically
        /// </summary>
        /// <param name="queueName">Queue Name</param>
        /// <returns>message content</returns>
        public string Read(string queueName, bool KeepAlive = true)
        {
            CheckConection(queueName);

            lock (this)
            {
                string msg = string.Empty;

                MQQueue queue;

                MQMessage queueMessage;
                MQGetMessageOptions queueGetMessageOptions;

                try
                {
                    queue = QueueManager.AccessQueue(queueName, MQC.MQOO_INPUT_AS_Q_DEF + MQC.MQOO_FAIL_IF_QUIESCING + MQC.MQOO_INQUIRE);
                    queueMessage = new MQMessage();
                    queueMessage.Format = MQC.MQFMT_STRING;
                    queueGetMessageOptions = new MQGetMessageOptions();

                    if (queue.CurrentDepth > 0)
                    {
                        queue.Get(queueMessage, queueGetMessageOptions);

                        if (queueMessage.MessageLength == 0)
                        {
                            return string.Empty;
                        }

                        msg = queueMessage.ReadString(queueMessage.MessageLength);
                    }
                }
                catch (Exception oException)
                {
                    GetErrorDetails(oException);
                    return string.Empty;
                }
                finally
                {
                    queue = null;
                    queueMessage = null;
                    queueGetMessageOptions = null;

                    if (!KeepAlive)
                    {
                        Close();
                    }
                }

                return msg;
            }
        }

        /// <summary>
        /// Tries to reconect in the IBM MQ
        /// </summary>
        public void TryReconnect()
        {
            this.Close();

            System.Threading.Thread.Sleep(500);

            try
            {
                this.Connect(this.Properties);
            }
            catch (Exception oException)
            {
                GetErrorDetails(oException);
            }
        }

        /// <summary>
        /// Gets the depth of the 
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="KeepAlive"></param>
        /// <returns></returns>
        public int GetDepth(string queueName, bool KeepAlive = true)
        {
            queueName.ThrowIfNull("The queue name cannot be null or empty");

            lock (this)
            {
                int retorno = -1;

                MQQueue queue;

                MQMessage queueMessage;
                MQGetMessageOptions queueGetMessageOptions;

                try
                {
                    queue = QueueManager.AccessQueue(queueName, MQC.MQOO_INPUT_AS_Q_DEF + MQC.MQOO_FAIL_IF_QUIESCING + MQC.MQOO_INQUIRE);
                    queueMessage = new MQMessage();
                    queueMessage.Format = MQC.MQFMT_STRING;
                    queueGetMessageOptions = new MQGetMessageOptions();

                    retorno = queue.CurrentDepth;
                }
                catch (MQException oException)
                {
                    // Broken Connection
                    if (oException.ReasonCode == 2009)
                    {
                        TryReconnect();
                    }
                    else
                    {
                        GetErrorDetails(oException);
                    }
                }
                catch (Exception oException)
                {
                    GetErrorDetails(oException);
                    return 0;
                }
                finally
                {
                    queue = null;
                    queueMessage = null;
                    queueGetMessageOptions = null;

                    if (!KeepAlive)
                    {
                        Close();
                    }
                }

                return retorno;
            }
        }

        /// <summary>
        /// Get the error details from the Exception object
        /// </summary>
        /// <param name="oException">Exception</param>
        private void GetErrorDetails(Exception oException)
        {
            this.HasError = true;
            this.ErrorMessage = oException.Message;
            this.Exception = oException;
        }

        /// <summary>
        /// Chech whether the hashtable contains all the key/value paired information properly informed
        /// </summary>
        private void CheckProperties()
        {
            if (this.Properties.IsNull() || this.Properties.Count == 0)
            {
                throw new ArgumentException("The hashtable that contains the properties is null or has no elements on it");
            }
            else
            {
                if (Properties[MQC.HOST_NAME_PROPERTY].IsNull() || Properties[MQC.CHANNEL_PROPERTY].IsNull() || Properties[MQC.PORT_PROPERTY].IsNull() || Properties[MQC.PORT_PROPERTY].ToString().IsInt() == false)
                {
                    throw new ArgumentException("The hashtable contains incorrect properties on it");
                }
            }
        }

        /// <summary>
        /// Check if the MQQueueManager instance is not null and is connected
        /// </summary>
        private void CheckConection(string queueName)
        {
            queueName.ThrowIfNull("The queue name cannot be null or empty");

            if (QueueManager.IsNull() || QueueManager.IsConnected == false)
            {
                throw new ArgumentNullException("The service was not able to connect to the IBM MQ Service or the QueueManager is no longer available");
            }
        }

        /// <summary>
        /// Close the Queue and dispose the MQ object
        /// </summary>
        private void Close()
        {
            if (QueueManager.IsNotNull() && QueueManager.IsConnected)
            {
                QueueManager.Commit();
                QueueManager.Disconnect();

                QueueManager = null;
            }
        }

        #endregion

        #region| IDisposable method |

        public void Dispose()
        {
            Close();
        }

        #endregion
    }
}
